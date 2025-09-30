import os
import json
import re
import time
import logging
import hashlib
import html
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta, time as dt_time
from dateutil import parser
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, Union
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from azure.data.tables import TableServiceClient
from openai import AzureOpenAI

load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class EmailProcessor:
    """이메일 처리 메인 클래스"""

    def __init__(self):
        """초기화 및 환경 변수 로드"""

        # 환경 변수 로드
        self.azure_openai_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.azure_openai_key = os.getenv("AZURE_OPENAI_KEY")
        self.azure_openai_deployment_chat = os.getenv(
            "AZURE_OPENAI_DEPLOYMENT_CHAT", "gpt-4"
        )
        self.azure_openai_deployment_emb = os.getenv(
            "AZURE_OPENAI_DEPLOYMENT_EMBEDDING", "text-embedding-3-small"
        )
        self.ai_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
        self.ai_search_index = os.getenv("AI_SEARCH_INDEX", "emails-index")
        self.ai_search_admin_key = os.getenv("AI_SEARCH_ADMIN_KEY")
        self.azure_storage_connection_string = os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        self.default_confidence = float(os.getenv("DEFAULT_CONFIDENCE", "0.65"))

        # 환경 변수 검증
        self._validate_environment()

        # 클라이언트 초기화
        self._initialize_clients()

        # 임베딩 배포명 자동 감지
        self._detect_embedding_deployment()

    def _validate_environment(self):
        """필수 환경 변수 검증"""

        required_vars = {
            "AZURE_OPENAI_ENDPOINT": self.azure_openai_endpoint,
            "AZURE_OPENAI_KEY": self.azure_openai_key,
            "AI_SEARCH_ENDPOINT": self.ai_search_endpoint,
            "AI_SEARCH_ADMIN_KEY": self.ai_search_admin_key,
            "AZURE_STORAGE_CONNECTION_STRING": self.azure_storage_connection_string,
        }

        missing_vars = [key for key, value in required_vars.items() if not value]

        if missing_vars:
            raise ValueError(f"필수 환경 변수가 설정되지 않았습니다: {missing_vars}")

        logging.info("✅ 모든 환경 변수 검증 완료")

    def _initialize_clients(self):
        """Azure 클라이언트 초기화"""

        try:
            # OpenAI 클라이언트
            self.openai_client = AzureOpenAI(
                azure_endpoint=self.azure_openai_endpoint,
                api_key=self.azure_openai_key,
                api_version="2024-02-01",
            )

            # AI Search 클라이언트
            self.search_client = SearchClient(
                endpoint=self.ai_search_endpoint,
                index_name=self.ai_search_index,
                credential=AzureKeyCredential(self.ai_search_admin_key),
            )

            # Table Storage 클라이언트
            self.table_service = TableServiceClient.from_connection_string(
                self.azure_storage_connection_string
            )

            logging.info("✅ 모든 Azure 클라이언트 초기화 완료")

        except Exception as e:
            logging.error(f"❌ 클라이언트 초기화 실패: {e}")
            raise

    def _detect_embedding_deployment(self):
        """임베딩 배포명 자동 감지"""

        # 일반적인 임베딩 배포명들
        possible_names = [
            "text-embedding-3-small",
            "text-embedding-ada-002",
            "embedding-3-small",
            "embedding",
            self.azure_openai_deployment_emb,
        ]

        for deployment_name in possible_names:
            try:
                logging.info(f"임베딩 배포명 테스트: {deployment_name}")
                response = self.openai_client.embeddings.create(
                    model=deployment_name, input=["테스트"]
                )

                if response.data:
                    self.azure_openai_deployment_emb = deployment_name
                    logging.info(f"✅ 임베딩 배포명 확인: {deployment_name}")
                    return

            except Exception as e:
                logging.warning(f"배포명 '{deployment_name}' 테스트 실패: {e}")
                continue

        # 모든 배포명 실패시 오류
        raise ValueError(
            "사용 가능한 임베딩 배포를 찾을 수 없습니다. AZURE_OPENAI_DEPLOYMENT_EMB 환경 변수를 확인하세요."
        )

    # ======================
    # 텍스트 전처리 / 힌트
    # ======================
    def _pre_extract_deadlines(self, text: str, max_items: int = 5) -> List[str]:
        """
        본문에서 한국어 기한 표현 후보를 뽑아 LLM에 힌트로 제공.
        """
        patterns = [
            # '까지' 있는 유형
            r"\(\s*\d{1,2}/\d{1,2}(?:\([^)]*\))?\s*까지\s*\)",
            r"\d{1,2}/\d{1,2}(?:\([^)]*\))?\s*까지",
            r"\d{4}-\d{1,2}-\d{1,2}(?:\s*\d{1,2}:\d{2})?\s*까지",
            r"(?:이번\s*주|금주)\s*(월|화|수|목|금|토|일)요일?\s*까지",
            r"(?:금일|오늘|내일|명일)\s*(?:오전|오후)?\s*\d{1,2}시(?:\s*\d{1,2}분)?\s*까지",
            r"(?:오전|오후)?\s*\d{1,2}시(?:\s*\d{1,2}분)?\s*까지",
            r"(?:금일|오늘|내일|명일)\s*까지",
            # '까지' 없는 흔한 마감/범위
            r"마감[:\s]*\d{1,2}/\d{1,2}(?:\([^)]*\))?",
            r"\b\d{1,2}/\d{1,2}\b(?:\s*\d{1,2}:\d{2})?",
            r"\d{4}-\d{1,2}-\d{1,2}",
            r"\d+\s*일\s*(?:후|뒤)",
            r"\b(?:EOD|EOW)\b",
            r"(업무\s*(?:종료|시간)\s*전)",
            r"\d{1,2}/\d{1,2}\s*~\s*\d{1,2}/\d{1,2}",
            r"\d{4}-\d{1,2}-\d{1,2}\s*~\s*\d{4}-\d{1,2}-\d{1,2}",
            # 주/월 내
            r"(이번\s*주\s*내|주중|이번\s*달\s*내|월말\s*까지|분기\s*말\s*까지)",
        ]
        found = []
        for p in patterns:
            for m in re.finditer(p, text, flags=re.IGNORECASE):
                s = m.group(0).strip()
                if s not in found:
                    found.append(s)
                    if len(found) >= max_items:
                        return found
        return found

    def _collect_deadline_hints(self, email: Dict) -> List[str]:
        text_blob = f"{email.get('subject','')}\n\n{email.get('body','')}".strip()
        return self._pre_extract_deadlines(text_blob, max_items=10)

    def _collect_deadline_hints_from_text(self, text: str) -> List[str]:
        return self._pre_extract_deadlines(text, max_items=10)

    def _find_context(self, text: str, snippet: str, width: int = 80) -> str:
        i = text.find(snippet)
        if i == -1:
            return ""
        start = max(0, i - width)
        end = min(len(text), i + len(snippet) + width)
        return text[start:end]


    # ======================
    # 멘션/세그먼트 로직
    # ======================
    def _is_self_mention_text(self, mention_text: str, user_context: dict) -> bool:
        """멘션 문자열이 나인지 판별"""
        name = (user_context.get("name") or "").strip()
        email = (user_context.get("email") or "").strip().lower()
        team = (user_context.get("team") or "").strip()

        # 멘션 원문 정리
        raw = mention_text.strip()
        if not raw.startswith("@"):
            return False

        # '@' 제거, 괄호 내용 제거 → "@박지훈(백엔드개발팀)" -> "박지훈"
        base = raw.lstrip("@").split("(", 1)[0]
        base = base.replace(" ", "").lower()
        # 존칭/불용어 제거
        base = re.sub(r"(님|씨|님들)$", "", base)

        packed = raw.replace(" ", "").lower()

        # 이메일 로컬 파트도 비교 (ex. jihoon.park)
        email_local = email.split("@")[0] if email else ""

        return any(
            [
                # 정확 이름 매칭 (공백 제거, 대소문자 무시)
                (name and base == name.replace(" ", "").lower()),
                # '@박지훈...' 형태 시작 매칭
                (name and packed.startswith("@" + name.replace(" ", "").lower())),
                # 이메일 포함
                (email and email in packed),
                # 이메일 로컬 파트 매칭
                (email_local and base == email_local),
                # 팀명 포함 (@백엔드개발팀)
                (team and team.replace(" ", "").lower() in packed),
            ]
        )

    def _get_self_mention_segments(
        self,
        text: str,
        user_context: dict,
        max_chars: int = 1500,
        max_lines: int = 25,
    ) -> List[Tuple[int, int, str]]:
        """
        내 멘션(또는 내가 포함된 멘션 클러스터)부터 다음 멘션 직전까지를 세그먼트로 반환.
        - 같은 줄에서 멘션이 연속 등장하고 간격 ≤ 80자면 같은 클러스터로 취급(공동지시).
        - 세그먼트 시작을 클러스터 시작에서 50자 앞(backoff)으로 당겨, 멘션 문맥이 LLM/검증 단계에 항상 추가되도록 보장.
        - 빈 줄에서 추가 컷, 길이 제한 유지.
        """
        mention_re = r"@[A-Za-z가-힣0-9_.\-]+(?:\([^)]+\))?"
        mentions = list(re.finditer(mention_re, text))
        if not mentions:
            return []

        CLUSTER_GAP = 80
        BACKOFF = 50  # 멘션 앞쪽 문맥 조금 포함

        segs: List[Tuple[int, int, str]] = []
        i = 0
        while i < len(mentions):
            # i부터 클러스터 구성(같은 줄 & GAP 이하)
            cluster = [mentions[i]]
            j = i + 1
            while j < len(mentions):
                gap = text[mentions[j - 1].end() : mentions[j].start()]
                if ("\n" not in gap) and (len(gap) <= CLUSTER_GAP):
                    cluster.append(mentions[j])
                    j += 1
                else:
                    break

            # 내가 포함된 클러스터만 세그먼트 대상
            if any(
                self._is_self_mention_text(m.group(0), user_context) for m in cluster
            ):
                cluster_start = cluster[0].start()
                cluster_end = cluster[-1].end()
                next_start = mentions[j].start() if j < len(mentions) else len(text)

                # 🔹 멘션을 포함시키고, 살짝 앞(backoff)까지 넣어준다
                seg_start = max(0, cluster_start - BACKOFF)
                seg_end = next_start

                seg = text[seg_start:seg_end]

                # 단락 경계(빈 줄)에서 컷
                m_blank = re.search(r"\n\s*\n", seg)
                if m_blank:
                    seg = seg[: m_blank.start()]

                # 길이 제한
                lines = seg.splitlines()
                if len(lines) > max_lines:
                    seg = "\n".join(lines[:max_lines])
                if len(seg) > max_chars:
                    seg = seg[:max_chars]

                segs.append((seg_start, seg_start + len(seg), seg))

            i = j

        return segs

    def _is_due_for_user(self, text: str, cand: str, user_context: dict) -> bool:
        """
        '나'에게 유효한 마감(due_raw)인지 판별.
        규칙:
        1) 내 멘션 ~ 다음 멘션 사이 구간에 cand가 있으면 내 것.
        2) 여러 멘션이 한 줄/짧은 간격(같은 문장)으로 묶인 '클러스터' 직후 cand가 나오면,
           그 클러스터에 내가 포함되어 있으면 내 것으로 간주(공동 지시).
        3) cand 직전 윈도우에서 마지막 멘션이 '나'라면 내 것.
        4) 멘션이 전혀 없으면 기존 완화 규칙.
        """
        name = (user_context.get("name") or "").strip()
        email = (user_context.get("email") or "").strip()
        team = (user_context.get("team") or "").strip()

        cand_idx = text.find(cand)
        if cand_idx == -1:
            return False

        # 모든 멘션 수집
        mention_re = r"@[A-Za-z가-힣0-9_.]+(?:\([^)]+\))?"
        mentions = list(re.finditer(mention_re, text))

        # 멘션이 없으면: 완화 규칙
        if not mentions:
            ctx = self._find_context(text, cand, width=80)
            return any(
                [
                    name and (name in ctx),
                    email and (email in ctx),
                    team and (team in ctx),
                    re.search(
                        r"(아래\s*작업|다음\s*작업).*(까지|마감|부탁|요청|확인)", ctx
                    ),
                ]
            )

        # 1) 기본: 내 멘션 ~ 다음 멘션 사이 구간
        for i, m in enumerate(mentions):
            if self._is_self_mention_text(m.group(0), user_context):
                seg_start = m.end()
                seg_end = (
                    mentions[i + 1].start() if i + 1 < len(mentions) else len(text)
                )
                if seg_start <= cand_idx < seg_end:
                    return True

        # 2) 멘션 클러스터(같은 문장/짧은 간격) 직후 cand → 클러스터에 내가 포함되어 있으면 True
        CLUSTER_GAP = 80
        last_before_idx = -1
        for i, m in enumerate(mentions):
            if m.start() < cand_idx:
                last_before_idx = i
            else:
                break

        if last_before_idx >= 0:
            cluster = [mentions[last_before_idx]]
            j = last_before_idx - 1
            while j >= 0:
                prev = mentions[j]
                gap_text = text[prev.end() : cluster[0].start()]
                if ("\n" not in gap_text) and (len(gap_text) <= CLUSTER_GAP):
                    cluster.insert(0, prev)
                    j -= 1
                else:
                    break

            cluster_end = cluster[-1].end()
            has_mention_between = any(
                m.start() >= cluster_end and m.start() < cand_idx for m in mentions
            )
            if not has_mention_between:
                if any(
                    self._is_self_mention_text(m.group(0), user_context)
                    for m in cluster
                ):
                    return True

        # 3) cand 직전 윈도우(200자)에서 마지막 멘션이 나
        window_start = max(0, cand_idx - 200)
        ctx = text[window_start:cand_idx]
        last_any = None
        for m in re.finditer(mention_re, ctx):
            last_any = m
        if last_any:
            if self._is_self_mention_text(last_any.group(0), user_context):
                tail = ctx[last_any.end() :]
                if ("\n" not in tail) or re.search(
                    r"(까지|마감|부탁|요청|확인|완료)", tail
                ):
                    return True

        return False

    # ======================
    # HTML → TEXT 전환
    # ======================
    def _html_to_text(self, html_str: str) -> str:
        if not html_str:
            return ""
        text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html_str)
        text = re.sub(r"(?is)<br\s*/?>", "\n", text)
        text = re.sub(r"(?is)</p>", "\n", text)
        text = re.sub(r"(?is)</li>", "\n- ", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)
        text = html.unescape(text)
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ======================
    # 이메일 표준화
    # ======================
    def preprocess_email(self, email_data: Dict) -> Dict:
        """이메일 데이터 전처리 (안전한 처리)"""

        # 안전한 필드 추출
        def safe_get(key: str, default: str = "") -> str:
            value = email_data.get(key)
            return str(value) if value is not None else default

        def safe_get_list(key: str, default_list: List = None) -> List:
            value = email_data.get(key)
            if isinstance(value, list):
                return value
            return default_list or []

        # 기본 정제
        body = safe_get("email_body")
        html_body = safe_get("html_body")

        # ✅ 항상 병합 (중복 줄 제거)
        if html_body:
            html_text = self._html_to_text(html_body)
            if html_text:
                merged = (body + "\n\n" + html_text).strip() if body else html_text
                # 중복 라인 간단 제거
                lines = []
                seen = set()
                for ln in merged.splitlines():
                    key = ln.strip()
                    if key and key not in seen:
                        lines.append(ln)
                        seen.add(key)
                body = "\n".join(lines)

        # 서명/광고 블록 제거 (간단한 휴리스틱)
        signature_patterns = [r"\n\n--\n.*", r"\n\n.*드림$", r"\n\n.*감사합니다\..*"]
        for pattern in signature_patterns:
            body = re.sub(pattern, "", body, flags=re.DOTALL | re.MULTILINE)

        # 주소록 배열 정규화
        to_names = safe_get_list("to_names")
        to_addresses = safe_get_list("to_addresses")
        cc_names = safe_get_list("cc_names")
        cc_addresses = safe_get_list("cc_addresses")

        max_to_len = max(len(to_names), len(to_addresses))
        max_cc_len = max(len(cc_names), len(cc_addresses))

        to_list = []
        for i in range(max_to_len):
            name = to_names[i] if i < len(to_names) else ""
            email = to_addresses[i] if i < len(to_addresses) else ""
            if name or email:
                to_list.append({"name": name, "email": email})

        cc_list = []
        for i in range(max_cc_len):
            name = cc_names[i] if i < len(cc_names) else ""
            email = cc_addresses[i] if i < len(cc_addresses) else ""
            if name or email:
                cc_list.append({"name": name, "email": email})

        threads = email_data.get("threads", {})
        keywords = []
        if isinstance(threads, dict):
            keywords = threads.get("keywords", [])
            if not isinstance(keywords, list):
                keywords = []

        standardized = {
            "recordId": safe_get("recordId"),
            "emailId": safe_get("email_id"),
            "subject": safe_get("subject"),
            "from": {"name": safe_get("from_name"), "email": safe_get("from_address")},
            "to": to_list,
            "cc": cc_list,
            "receivedAt": safe_get("date"),
            "body": body.strip(),
            "html_body": html_body,
            "conversationId": safe_get("thread_id"),
            "priority_hint": safe_get("priority"),
            "keywords": keywords,
        }

        return standardized

    # ======================
    # 정책 엔진
    # ======================
    def analyze_with_policy_engine(self, email_data: Dict, user_context: Dict) -> Dict:
        """정책 엔진 분석 (안전한 처리)"""

        # 안전한 필드 추출
        from_email = email_data.get("from_address", "")
        to_emails = email_data.get("to_addresses", [])
        cc_emails = email_data.get("cc_addresses", [])
        body = email_data.get("email_body", "")

        if not isinstance(to_emails, list):
            to_emails = []
        if not isinstance(cc_emails, list):
            cc_emails = []
        if not isinstance(body, str):
            body = ""
        if not isinstance(from_email, str):
            from_email = ""

        user_name = user_context.get("name", "")
        user_email = user_context.get("email", "")
        user_team = user_context.get("team", "")

        mentions = []
        if body:
            try:
                mentions = re.findall(r"@(\S+(?:\([^)]+\))?)", body)
                mentions = [f"@{mention}" for mention in mentions]
            except Exception as e:
                logging.warning(f"멘션 추출 실패: {e}")
                mentions = []

        request_keywords = [
            "부탁",
            "요청",
            "확인",
            "검토",
            "승인",
            "회신",
            "즉시",
            "긴급",
            "마감",
            "완료",
            "해주세요",
            "바랍니다",
            "처리",
            "대응",
            "분석",
            "점검",
            "실행",
        ]
        request_detected = False
        if body:
            try:
                request_detected = any(keyword in body for keyword in request_keywords)
            except Exception as e:
                logging.warning(f"요청 키워드 감지 실패: {e}")
                request_detected = False

        self_sent = bool(from_email and user_email and from_email == user_email)
        to_contains_self = bool(user_email and user_email in to_emails)
        cc_contains_self = bool(user_email and user_email in cc_emails)

        policy_decision = "none"
        try:
            if to_contains_self and request_detected:
                user_mentions = [
                    mention
                    for mention in mentions
                    if user_name and f"@{user_name}" in mention
                ]
                if (
                    not any(
                        mention for mention in mentions if mention != f"@{user_name}"
                    )
                ) or user_mentions:
                    policy_decision = "A"
            elif cc_contains_self and not to_contains_self:
                if any(
                    user_name and f"@{user_name}" in mention for mention in mentions
                ):
                    policy_decision = "A"  # 명시적 지목이면 액션
                else:
                    policy_decision = "B"
            elif self_sent and request_detected:
                policy_decision = "C"
            elif (
                to_contains_self
                and user_team
                and user_team in body
                and request_detected
            ):
                policy_decision = "D"
        except Exception as e:
            logging.warning(f"정책 결정 중 오류: {e}")
            policy_decision = "none"

        return {
            "policy_decision": policy_decision,
            "self_sent": self_sent,
            "to_contains_self": to_contains_self,
            "cc_contains_self": cc_contains_self,
            "mentions": mentions,
            "request_detected": request_detected,
        }

    # ======================
    # 세그먼트 전용 LLM 프롬프트/검증
    # ======================
    def _build_action_prompt_for_segment(
        self,
        email_data: Dict,
        policy_signals: Dict,
        user_context: Dict,
        segment_text: str,
        deadline_hints: List[str],
    ) -> Tuple[str, str]:
        name = user_context["name"]
        email = user_context["email"]
        team = user_context["team"]

        followup_hint = ""
        if policy_signals.get("self_sent"):
            # 🔸 내가 보낸 메일이라면 FOLLOW_UP 모드 강제
            followup_hint = (
                "\n- 이 메일은 내가 보낸 요청이므로 action.type은 반드시 FOLLOW_UP 입니다."
                "\n- FOLLOW_UP에서는 '상대에게 요청한 핵심 작업'을 title로 12~20자로 요약하세요(예: \"로그 분석 결과 회신 요청\")."
                "\n- assignee_candidates에는 내 주소가 아니라 '상대 수신자/팀'을 넣으세요."
                "\n- due_raw는 세그먼트(또는 이 세그먼트 안에서 보이는 문장)에서 발견되는 기한 표현을 그대로 복사하세요(없으면 null)."
            )

        system_prompt = f"""
    당신은 이메일에서 '수신자 {name}<{email}>' 또는 '{team}' 팀(그리고 {name}이 To에 포함)에
    실제로 배정된 액션만 추출합니다. JSON 한 줄만 출력하세요(요약/설명/코드블록 금지).

    규칙:
    - 이 프롬프트는 '세그먼트' 텍스트만 제공합니다. 반드시 '세그먼트 범위 내'에서만 액션을 추출하세요.
    - '배정됨' = (내 이메일 To) 또는 (@{name} 멘션/내가 포함된 멘션 클러스터) 또는 (팀단위 지시 + To에 내가 포함).
    - title: 12~20자, 동사+명사(예: "API 로그 분석").
    - due_raw: 원문 그대로 복사(예: "금일 오후 2시까지"). 세그먼트 밖은 절대 보지 마세요.
    - 값이 없으면 null.{followup_hint}

    - JSON 스키마:
    {{"is_action":true/false,"policy_decision":"A|B|C|D|none",
    "action":{{"type":"DO|FOLLOW_UP|NONE","title":"", "assignee_candidates":["이름 <이메일>","팀명"],"due_raw":null,"priority":"High|Medium|Low","tags":["태그1","태그2"],"rationale":""}}}}
    """.strip()

        user_prompt = f"""
    [세그먼트 전용 본문]
    {segment_text[:3000]}

    [세그먼트 내 기한 후보 힌트]: {deadline_hints}

    정책 신호:
    - 정책 결정: {policy_signals['policy_decision']}
    - 본인 발송: {policy_signals['self_sent']}
    - To에 본인 포함: {policy_signals['to_contains_self']}
    - 멘션: {policy_signals['mentions']}
    - 요청 감지: {policy_signals['request_detected']}

    주의: 오직 JSON 한 줄만 출력하세요.
    """.strip()

        return system_prompt, user_prompt

    def _validate_and_fix_action(
        self,
        result: Dict,
        context_text: str,
        hints: List[str],
        policy_signals: Dict,
        user_context: Dict,
    ) -> Dict:
        if not isinstance(result, dict):
            return {"is_action": False, "policy_decision": "none", "action": None}

        is_action = bool(result.get("is_action"))
        policy = result.get("policy_decision") or policy_signals.get(
            "policy_decision", "none"
        )
        action = result.get("action") or {}

        a_type = (action.get("type") or "NONE").upper()
        if a_type not in {"DO", "FOLLOW_UP", "NONE"}:
            a_type = "NONE"

        # 🔸 내가 보낸 메일이면 무조건 FOLLOW_UP로 교정
        if policy_signals.get("self_sent"):
            a_type = "FOLLOW_UP"
            is_action = True

        title = (action.get("title") or "").strip()
        if len(title) > 20:
            title = title[:20].rstrip()

        priority = action.get("priority") or "Medium"
        tags = action.get("tags") or []
        if not isinstance(tags, list):
            tags = [str(tags)]
        tags = list(dict.fromkeys([str(t) for t in tags]))

        assignees = action.get("assignee_candidates") or []

        due_raw = (action.get("due_raw") or "").strip() or None
        # 🔸 FOLLOW_UP은 내 due 맥락 검증에서 제외(요청 상대의 기한일 수 있음)
        if due_raw and a_type != "FOLLOW_UP":
            if not self._is_due_for_user(context_text, due_raw, user_context):
                logging.info(
                    "🚫 타인 지시 맥락으로 due_raw 무효화(세그먼트 검증): %s", due_raw
                )
                due_raw = None

        if a_type == "NONE":
            is_action = False
            action_out = None
        else:
            action_out = {
                "type": a_type,
                "title": title,
                "assignee_candidates": assignees,
                "due_raw": due_raw,
                "priority": priority,
                "tags": tags,
                "rationale": action.get("rationale", ""),
            }

        return {"is_action": is_action, "policy_decision": policy, "action": action_out}

    # ======================
    # LLM 추출 (세그먼트 기반)
    # ======================
    def extract_actions_with_llm(
        self, email_data: Dict, policy_signals: Dict, user_context: Dict
    ) -> Dict:
        """
        1) 본문에서 내 멘션/클러스터 기반 '세그먼트'를 자름
        2) 각 세그먼트에 대해 LLM JSON 추출
        3) 첫 유효 액션 반환(없으면 전체 본문으로 1회 폴백)
        """
        full_subject = email_data.get("subject", "")
        full_body = email_data.get("body", "")
        text_blob_full = f"{full_subject}\n\n{full_body}"

        segments = self._get_self_mention_segments(full_body, user_context)
        tried_any = False

        def _postfix(result: Dict, seg_text: str, hints: List[str]) -> Dict:
            return self._validate_and_fix_action(
                result,
                f"{full_subject}\n\n{seg_text}",
                hints,
                policy_signals,
                user_context,
            )

        # 1) 세그먼트별 시도
        for idx, (_, _, seg_text) in enumerate(segments):
            tried_any = True
            hints = self._collect_deadline_hints_from_text(seg_text)
            sys_p, usr_p = self._build_action_prompt_for_segment(
                email_data, policy_signals, user_context, seg_text, hints
            )

            try:
                logging.info(
                    "=== 📤 LLM 요청 (segment #%d system) ===\n%s", idx + 1, sys_p
                )
                logging.info(
                    "=== 📤 LLM 요청 (segment #%d user) ===\n%s", idx + 1, usr_p
                )

                resp = self.openai_client.chat.completions.create(
                    model=self.azure_openai_deployment_chat,
                    messages=[
                        {"role": "system", "content": sys_p},
                        {"role": "user", "content": usr_p},
                    ],
                    temperature=0.1,
                    max_tokens=600,
                )
                raw = (resp.choices[0].message.content or "").strip()
                logging.info("=== 📥 LLM 응답 (segment #%d) ===\n%s", idx + 1, raw)

                # JSON만 추출
                m = re.search(r"\{.*\}\s*$", raw, flags=re.DOTALL)
                if m:
                    raw = m.group(0)
                result = json.loads(raw)

                result = _postfix(result, seg_text, hints)
                if result.get("is_action") and result.get("action"):
                    logging.info("✅ 세그먼트 #%d 에서 액션 확정", idx + 1)
                    return result

            except Exception as e:
                logging.warning("세그먼트 #%d 처리 실패: %s", idx + 1, e)

        # 2) 세그먼트가 없거나 다 실패 → 전체 본문으로 마지막 1회 시도
        if not tried_any:
            deadline_hints = self._collect_deadline_hints(email_data)
            sys_p, usr_p = self._build_action_prompt_for_segment(
                email_data, policy_signals, user_context, full_body, deadline_hints
            )
            try:
                logging.info("=== 📤 LLM 요청 (fallback system) ===\n%s", sys_p)
                logging.info("=== 📤 LLM 요청 (fallback user) ===\n%s", usr_p)
                resp = self.openai_client.chat.completions.create(
                    model=self.azure_openai_deployment_chat,
                    messages=[
                        {"role": "system", "content": sys_p},
                        {"role": "user", "content": usr_p},
                    ],
                    temperature=0.1,
                    max_tokens=600,
                )
                raw = (resp.choices[0].message.content or "").strip()
                logging.info("=== 📥 LLM 응답 (fallback) ===\n%s", raw)
                m = re.search(r"\{.*\}\s*$", raw, flags=re.DOTALL)
                if m:
                    raw = m.group(0)
                result = json.loads(raw)
                result = self._validate_and_fix_action(
                    result, text_blob_full, deadline_hints, policy_signals, user_context
                )
                logging.info(
                    "✅ LLM 액션 추출 완료: %s", result.get("is_action", False)
                )
                return result
            except Exception as e:
                logging.exception("❌ LLM 추출 실패(폴백)")
                return {"is_action": False, "policy_decision": "none", "action": None}

        # 세그먼트는 있었지만 모두 비액션/실패
        return {
            "is_action": False,
            "policy_decision": policy_signals.get("policy_decision", "none"),
            "action": None,
        }

    # ======================
    # 마감 해석(KST/UTC) + LLM 보정
    # ======================
    def _llm_resolve_deadline(
        self, due_raw: str, received_at_iso: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        규칙 파싱이 안 될 때 LLM로 상대표현을 절대시간으로 보정.
        반환: (resolved_kst_str "YYYY-MM-DD HH:MM KST", resolved_utc_iso) 또는 (None, None)
        """
        try:
            kst = ZoneInfo("Asia/Seoul")
            now_kst = None
            if received_at_iso:
                tmp = parser.parse(received_at_iso)
                now_kst = tmp.astimezone(kst) if tmp.tzinfo else tmp.replace(tzinfo=kst)
            if not now_kst:
                now_kst = datetime.now(kst)

            system_prompt = (
                "너는 한국어 기한 표현을 KST 기준의 명확한 날짜/시간으로 변환하는 도우미야.\n"
                '- 출력은 반드시 JSON 한 줄: {"kst":"YYYY-MM-DD HH:MM","iso":"YYYY-MM-DDTHH:MM:SSZ"}\n'
                "- 시간이 없으면 18:00으로 가정.\n"
                "- '금일/오늘'=수신일, '명일/내일'=+1, '모레'=+2.\n"
                "- '이번 주 금요일'=수신일이 속한 주의 금요일.\n"
                "- '다음 주/차주 화요일'=다음 주의 화요일.\n"
                "- 불가능하면 두 값 모두 null."
            )
            user_prompt = (
                f"원문: {due_raw}\n"
                f"수신시각(KST): {now_kst.strftime('%Y-%m-%d %H:%M:%S')}\n"
                "한 줄 JSON으로만 답해."
            )

            resp = self.openai_client.chat.completions.create(
                model=self.azure_openai_deployment_chat,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.0,
                max_tokens=120,
            )
            raw = (resp.choices[0].message.content or "").strip()
            data = json.loads(raw)

            kst_str = data.get("kst")
            iso = data.get("iso")
            if (
                kst_str
                and re.match(r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$", kst_str)
                and iso
                and iso.endswith("Z")
            ):
                return f"{kst_str} KST", iso
        except Exception as e:
            logging.info(f"LLM 기한 보정 실패: {e}")
        return None, None

    def _resolve_relative_deadline(
        self, due_raw: str, received_at_iso: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        상대/모호 표현(due_raw)을 KST/UTC로 해석.
        우선 규칙으로 시도, 실패 시 _llm_resolve_deadline()으로 보정.
        반환: (resolved_kst_str 'YYYY-MM-DD HH:MM KST', resolved_utc_iso)
        """
        if not due_raw:
            return None, None

        kst = ZoneInfo("Asia/Seoul")
        # 기준시각: 수신시각이 있으면 그것, 없으면 now
        try:
            if received_at_iso:
                base = parser.parse(received_at_iso)
                now_kst = (
                    base.astimezone(kst) if base.tzinfo else base.replace(tzinfo=kst)
                )
            else:
                now_kst = datetime.now(kst)
        except Exception:
            now_kst = datetime.now(kst)

        text = due_raw.strip()

        # 기본 시간(미지정 시 18:00)
        hour = 18
        minute = 0

        # 오전/오후 시:분
        t = re.search(r"(오전|오후)?\s*(\d{1,2})시(?:\s*(\d{1,2})분)?", text)
        if t:
            ampm, hh, mm = t.groups()
            hour = int(hh)
            minute = int(mm) if mm else 0
            if ampm == "오후" and hour < 12:
                hour += 12
            if ampm == "오전" and hour == 12:
                hour = 0

        wd_map = {"월": 0, "화": 1, "수": 2, "목": 3, "금": 4, "토": 5, "일": 6}
        target_date = None

        # 오늘/금일/명일/내일/모레
        if re.search(r"(금일|오늘)", text):
            target_date = now_kst.date()
        elif re.search(r"(명일|내일)", text):
            target_date = (now_kst + timedelta(days=1)).date()
        elif "모레" in text:
            target_date = (now_kst + timedelta(days=2)).date()

        # 이번 주 요일까지
        if not target_date:
            m = re.search(
                r"(?:이번\s*주|금주)\s*(월|화|수|목|금|토|일)요일?\s*까지?", text
            )
            if m:
                wd = m.group(1)
                delta = (wd_map[wd] - now_kst.weekday()) % 7
                target_date = (now_kst + timedelta(days=delta)).date()

        # 다음 주/차주 요일까지
        if not target_date:
            m = re.search(
                r"(?:다음\s*주|차주)\s*(월|화|수|목|금|토|일)요일?\s*까지?", text
            )
            if m:
                wd = m.group(1)
                delta_to_monday = (0 - now_kst.weekday()) % 7
                next_monday = (
                    now_kst + timedelta(days=delta_to_monday)
                ).date() + timedelta(days=7)
                target_date = next_monday + timedelta(days=wd_map[wd])

        # EOD/EOW
        if not target_date:
            if re.search(r"\bEOD\b", text, re.IGNORECASE):
                target_date = now_kst.date()
                hour, minute = 18, 0
            elif re.search(r"\bEOW\b", text, re.IGNORECASE):
                delta = (4 - now_kst.weekday()) % 7  # 금요일
                target_date = (now_kst + timedelta(days=delta)).date()
                hour, minute = 18, 0

        # YYYY-MM-DD
        if not target_date:
            m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
            if m:
                y, mo, d = map(int, m.groups())
                target_date = datetime(y, mo, d, tzinfo=kst).date()

        # MM/DD
        if not target_date:
            m = re.search(r"\b(\d{1,2})/(\d{1,2})\b", text)
            if m:
                mo, d = map(int, m.groups())
                y = now_kst.year if mo >= now_kst.month else now_kst.year + 1
                target_date = datetime(y, mo, d, tzinfo=kst).date()

        # N일 후/뒤
        if not target_date:
            m = re.search(r"(\d+)\s*일\s*(?:후|뒤)", text)
            if m:
                days = int(m.group(1))
                target_date = (now_kst + timedelta(days=days)).date()

        # 마지막 수단: dateutil
        if not target_date:
            try:
                parsed = parser.parse(text, fuzzy=True)
                parsed = (
                    parsed.astimezone(kst)
                    if parsed.tzinfo
                    else parsed.replace(tzinfo=kst)
                )
                target_date = parsed.date()
                hour = parsed.hour or hour
                minute = parsed.minute or minute
            except Exception:
                pass

        # 규칙으로도 못 구하면 LLM 보정
        if not target_date:
            return self._llm_resolve_deadline(
                due_raw=text, received_at_iso=received_at_iso
            )

        due_kst = datetime.combine(target_date, dt_time(hour, minute, tzinfo=kst))
        due_utc_iso = due_kst.astimezone(timezone.utc).isoformat()
        resolved_kst_str = due_kst.strftime("%Y-%m-%d %H:%M KST")
        return resolved_kst_str, due_utc_iso

    # ======================
    # 액션 정규화
    # ======================
    def normalize_action(self, raw_action: Dict, email_data: Dict) -> Optional[Dict]:
        """액션 데이터 정규화 (규칙→LLM 보정으로 due 해석, KST/UTC 동시 제공)"""

        if not raw_action.get("is_action") or not raw_action.get("action"):
            return None

        action = raw_action["action"]
        due_raw = (action.get("due_raw") or "").strip()

        # ----------------------
        # 담당자 결정 (FOLLOW_UP 보강)
        # ----------------------
        def _fmt_person(p: Dict[str, str]) -> Optional[str]:
            nm = (p.get("name") or "").strip()
            em = (p.get("email") or "").strip()
            if nm and em:
                return f"{nm} <{em}>"
            return em or (nm if nm else None)

        assignee: Optional[str] = None

        # 1) LLM 후보 중 이메일(@)이 있는 것을 우선 선택
        for cand in action.get("assignee_candidates") or []:
            if cand and "@" in cand:
                assignee = cand.strip()
                break

        # 2) FOLLOW_UP이면 To/CC에서 첫 대상(보낸이/비어있는 항목 제외)으로 지정
        if not assignee and action.get("type") == "FOLLOW_UP":
            sender_email = ((email_data.get("from") or {}).get("email") or "").strip()
            # To 우선, 없으면 CC
            for p in email_data.get("to") or []:
                s = _fmt_person(p)
                if s and (sender_email not in s):
                    assignee = s
                    break
            if not assignee:
                for p in email_data.get("cc") or []:
                    s = _fmt_person(p)
                    if s and (sender_email not in s):
                        assignee = s
                        break

        # 3) 후보에 이메일이 없었지만 텍스트가 있으면 그걸 사용
        if not assignee:
            for cand in action.get("assignee_candidates") or []:
                if cand and cand.strip():
                    assignee = cand.strip()
                    break

        if not assignee:
            assignee = "미지정"

        # ----------------------
        # 기본 신뢰도
        # ----------------------
        confidence = self.default_confidence

        # ----------------------
        # 기한 해석
        # ----------------------
        # 0) LLM 단계에서 이미 넣어둔 해석값이 있으면 우선 사용
        due_iso = action.get("due_resolved_iso")
        due_kst_str = action.get("due_resolved_kst")

        # 1) 없으면 규칙 기반(+LLM 보정 fallback)으로 해석
        if not due_iso and due_raw:
            rkst, risco = self._resolve_relative_deadline(
                due_raw, email_data.get("receivedAt")
            )
            if risco:
                due_iso = risco
                due_kst_str = rkst
                action["due_resolved_iso"] = risco
                action["due_resolved_kst"] = rkst

        # 2) 여전히 없으면(예외) 보수적 파싱 백업
        if not due_iso and due_raw:
            try:
                kst = ZoneInfo("Asia/Seoul")
                now_kst = datetime.now(kst)
                hour = 18
                minute = 0
                t = re.search(r"(오전|오후)?\s*(\d{1,2})시(?:\s*(\d{1,2})분)?", due_raw)
                if t:
                    ampm, hh, mm = t.groups()
                    hour = int(hh)
                    minute = int(mm) if mm else 0
                    if ampm == "오후" and hour < 12:
                        hour += 12
                    if ampm == "오전" and hour == 12:
                        hour = 0

                target_date = None
                if re.search(r"(금일|오늘)", due_raw):
                    target_date = now_kst.date()
                elif "내일" in due_raw or "명일" in due_raw:
                    target_date = (now_kst + timedelta(days=1)).date()
                elif re.search(
                    r"(?:이번\s*주|금주)\s*(월|화|수|목|금|토|일)요일?\s*까지", due_raw
                ):
                    wd_map = {
                        "월": 0,
                        "화": 1,
                        "수": 2,
                        "목": 3,
                        "금": 4,
                        "토": 5,
                        "일": 6,
                    }
                    wd = re.search(
                        r"(?:이번\s*주|금주)\s*(월|화|수|목|금|토|일)", due_raw
                    ).group(1)
                    delta = (wd_map[wd] - now_kst.weekday()) % 7
                    target_date = (now_kst + timedelta(days=delta)).date()
                elif re.search(r"\d{4}-\d{1,2}-\d{1,2}", due_raw):
                    y, m, d = map(
                        int, re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", due_raw).groups()
                    )
                    target_date = datetime(y, m, d, tzinfo=kst).date()
                elif re.search(r"\b\d{1,2}/\d{1,2}\b", due_raw):
                    m, d = map(
                        int, re.search(r"\b(\d{1,2})/(\d{1,2})\b", due_raw).groups()
                    )
                    y = now_kst.year if m >= now_kst.month else now_kst.year + 1
                    target_date = datetime(y, m, d, tzinfo=kst).date()
                elif re.search(r"\d+\s*일\s*(?:후|뒤)", due_raw):
                    days = int(re.search(r"(\d+)\s*일\s*(?:후|뒤)", due_raw).group(1))
                    target_date = (now_kst + timedelta(days=days)).date()

                if not target_date:
                    try:
                        parsed = parser.parse(due_raw, fuzzy=True)
                        parsed = (
                            parsed.astimezone(kst)
                            if parsed.tzinfo
                            else parsed.replace(tzinfo=kst)
                        )
                        target_date = parsed.date()
                        hour = parsed.hour or hour
                        minute = parsed.minute or minute
                    except Exception:
                        pass

                if target_date:
                    due_kst = datetime.combine(
                        target_date, dt_time(hour, minute, tzinfo=kst)
                    )
                    due_iso = due_kst.astimezone(timezone.utc).isoformat()
                    due_kst_str = due_kst.strftime("%Y-%m-%d %H:%M KST")
                    action.setdefault("due_resolved_kst", due_kst_str)
                    action.setdefault("due_resolved_iso", due_iso)
            except Exception as e:
                logging.error(f"날짜/시간 정규화 오류: {e}, due_raw: {due_raw}")
                due_iso = None

        # ----------------------
        # 신뢰도 보정
        # ----------------------
        if action.get("type") == "DO" and due_iso and "@" in assignee:
            confidence = min(confidence + 0.2, 1.0)
        elif action.get("type") == "FOLLOW_UP" and due_iso:
            confidence = min(confidence + 0.15, 1.0)

        # ----------------------
        # 노트
        # ----------------------
        note_parts = []
        if due_raw:
            note_parts.append(f"원본 기한: {due_raw}")
        if due_kst_str:
            note_parts.append(f"해석(KST): {due_kst_str}")

        return {
            "title": action.get("title", ""),
            "assignee": assignee,
            "due": due_iso,  # UTC ISO
            "priority": action.get("priority", "Medium"),
            "tags": action.get("tags", []),
            "type": action.get("type", "DO"),
            "confidence": confidence,
            "notes": " | ".join(note_parts) if note_parts else "",
        }

    # ======================
    # 청킹/임베딩/업로드
    # ======================
    def create_text_chunks(
        self, text: str, chunk_size: int = 900, overlap: int = 150
    ) -> List[str]:
        """텍스트 청킹"""

        if len(text) <= chunk_size:
            return [text]

        chunks = []
        start = 0

        while start < len(text):
            end = start + chunk_size

            # 문장 경계에서 자르기 시도
            if end < len(text):
                last_period = text.rfind(".", start, end)
                last_newline = text.rfind("\n", start, end)

                boundary = max(last_period, last_newline)
                if boundary > start + chunk_size // 2:
                    end = boundary + 1

            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)

            start = end - overlap

        return chunks

    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """텍스트 임베딩 생성"""

        try:
            response = self.openai_client.embeddings.create(
                model=self.azure_openai_deployment_emb, input=texts
            )

            embeddings = [data.embedding for data in response.data]
            logging.info(f"✅ 임베딩 생성 완료: {len(embeddings)}개")
            return embeddings

        except Exception as e:
            logging.error(f"❌ 임베딩 생성 실패: {e}")
            # 임베딩 실패시 0으로 채운 더미 벡터 반환
            return [[0.0] * 1536] * len(texts)

    def upload_to_search(self, email_data: Dict, action_data: Optional[Dict]) -> None:
        """Azure AI Search에 문서 업로드"""

        # 텍스트 청킹
        full_text = f"{email_data['subject']}\n\n{email_data['body']}"
        chunks = self.create_text_chunks(full_text)

        # 임베딩 생성
        embeddings = self.get_embeddings(chunks)

        # 검색 문서 생성
        documents = []

        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            # 원본 키에서 특수문자 제거
            raw_doc_id = f"{email_data['emailId']}::{i}"
            doc_id = self._sanitize_document_key(raw_doc_id)

            logging.info(f"문서 키 변환: '{raw_doc_id}' → '{doc_id}'")

            document = {
                "@search.action": "mergeOrUpload",
                "id": doc_id,
                "emailId": email_data["emailId"],
                "conversationId": email_data["conversationId"],
                "subject": email_data["subject"],
                "from_name": email_data["from"]["name"],
                "from_email": email_data["from"]["email"],
                "to_names": [p["name"] for p in email_data["to"]],
                "cc_names": [p["name"] for p in email_data["cc"]],
                "receivedAt": email_data["receivedAt"],
                "chunk": chunk,
                "bodyPreview": (
                    email_data["body"][:200] + "..."
                    if len(email_data["body"]) > 200
                    else email_data["body"]
                ),
                "chunkEmbedding": embedding,
                "webLink": "",
                "html_body": email_data.get("html_body", ""),
            }

            # 액션 데이터가 있으면 추가
            if action_data:
                document.update(
                    {
                        "action": action_data.get("title", ""),
                        "action_type": action_data.get("type", ""),
                        "assignee": action_data.get("assignee", ""),
                        "due": action_data.get("due"),  # UTC ISO or None
                        "priority": action_data.get("priority", ""),
                        "tags": action_data.get("tags", []),
                        "confidence": action_data.get("confidence", 0.0),
                    }
                )

            documents.append(document)

        # 배치 업로드
        try:
            result = self.search_client.upload_documents(documents)
            logging.info(f"✅ Search 인덱스 업로드 완료: {len(documents)}개 문서")
            return result

        except Exception as e:
            logging.error(f"❌ Search 인덱스 업로드 실패: {e}")
            raise

    def _sanitize_document_key(self, key: str) -> str:
        """Azure Search 문서 키 정제"""
        sanitized = re.sub(r"[^a-zA-Z0-9_\-=]", "_", key)
        sanitized = re.sub(r"_+", "_", sanitized)
        sanitized = sanitized.strip("_")
        if len(sanitized) > 1000:
            hash_suffix = hashlib.md5(key.encode()).hexdigest()[:8]
            sanitized = sanitized[:992] + "_" + hash_suffix
        return sanitized

    def save_to_table_storage(self, action_data: Dict, email_data: Dict) -> None:
        """Actions 테이블에 저장"""

        if not action_data:
            return

        try:
            actions_table = self.table_service.get_table_client("Actions")

            # RowKey도 정제
            raw_row_key = f"{email_data['emailId']}::0"
            row_key = self._sanitize_document_key(raw_row_key)

            entity = {
                "PartitionKey": "techcorp",
                "RowKey": row_key,
                "subject": email_data["subject"],
                "title": action_data.get("title", ""),
                "assignee": action_data.get("assignee", ""),
                "due": action_data.get("due", ""),  # UTC ISO
                "priority": action_data.get("priority", ""),
                "type": action_data.get("type", ""),
                "tags": ";".join(action_data.get("tags", [])),
                "confidence": action_data.get("confidence", 0.0),
                "receivedAt": email_data["receivedAt"],
                "conversationId": email_data.get("conversationId", ""),
                "webLink": "",
                "done": False,
            }

            actions_table.upsert_entity(entity)
            logging.info(f"✅ Actions 테이블 저장 완료: {action_data['title']}")

        except Exception as e:
            logging.error(f"❌ Actions 테이블 저장 실패: {e}")

    # ======================
    # 파이프라인
    # ======================
    def load_email_data(self, file_path: str) -> List[Dict]:
        """이메일 JSON 파일 로드"""

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            emails = data.get("values", [])
            logging.info(f"📧 {len(emails)}개 이메일 로드 완료: {file_path}")
            return emails

        except Exception as e:
            logging.error(f"❌ 이메일 데이터 로드 실패: {e}")
            raise

    def process_emails(self, email_file_path: str) -> Dict:
        """이메일 배치 처리"""

        logging.info(f"🚀 이메일 처리 시작: {email_file_path}")

        # 이메일 데이터 로드
        emails = self.load_email_data(email_file_path)

        # 처리 통계
        stats = {
            "total_emails": len(emails),
            "processed_emails": 0,
            "actions_extracted": 0,
            "errors": [],
        }

        # 각 이메일 처리
        for item in emails:
            try:
                # 안전한 데이터 추출
                if not isinstance(item, dict):
                    logging.warning(f"⚠️ 잘못된 아이템 형식 건너뜀: {type(item)}")
                    continue

                email_data = item.get("data")
                record_id = item.get("recordId", "unknown")

                # data 필드 검증
                if not email_data:
                    logging.warning(f"⚠️ data 필드가 없는 레코드 건너뜀: {record_id}")
                    continue

                if not isinstance(email_data, dict):
                    logging.warning(
                        f"⚠️ data 필드가 딕셔너리가 아닌 레코드 건너뜀: {record_id}"
                    )
                    continue

                # 필수 필드 검증
                required_fields = ["subject", "email_body", "from_address"]
                missing_fields = [
                    field for field in required_fields if not email_data.get(field)
                ]

                if missing_fields:
                    logging.warning(
                        f"⚠️ 필수 필드 누락으로 건너뜀 {record_id}: {missing_fields}"
                    )
                    continue

                # 1. 전처리
                standardized_email = self.preprocess_email(email_data)
                logging.info(f"📧 처리 중: {standardized_email['subject']}")

                # 2. 각 수신자별로 개인화된 분석 (샘플로 박지훈 기준)
                user_context = {
                    "name": "박지훈",
                    "email": "jihoon.park@techcorp.com",
                    "team": "백엔드개발팀",
                }

                # 3. 정책 엔진 적용 (원본 바디 사용)
                policy_signals = self.analyze_with_policy_engine(
                    email_data, user_context
                )
                logging.info(f"📋 정책 분석: {policy_signals['policy_decision']}")

                # 4. LLM 액션 추출(세그먼트 기반)
                action_result = self.extract_actions_with_llm(
                    standardized_email, policy_signals, user_context
                )

                # 5. 액션 정규화(마감 해석 KST/UTC)
                normalized_action = None
                if action_result.get("is_action"):
                    normalized_action = self.normalize_action(
                        action_result, standardized_email
                    )
                    if normalized_action:
                        stats["actions_extracted"] += 1
                        logging.info(f"⚡ 최종 보정 완료: {normalized_action}")

                # 6. Azure AI Search 업로드
                self.upload_to_search(standardized_email, normalized_action)

                # 7. Actions 테이블 저장
                if normalized_action:
                    self.save_to_table_storage(normalized_action, standardized_email)

                stats["processed_emails"] += 1

            except Exception as e:
                error_msg = f"이메일 처리 실패: {record_id} - {e}"
                logging.error(f"❌ {error_msg}")
                stats["errors"].append(error_msg)

        # 처리 결과 요약
        logging.info("🎉 이메일 처리 완료!")
        logging.info(f"📊 처리 통계:")
        logging.info(f"   - 총 이메일: {stats['total_emails']}개")
        logging.info(f"   - 처리 성공: {stats['processed_emails']}개")
        logging.info(f"   - 액션 추출: {stats['actions_extracted']}개")
        logging.info(f"   - 오류: {len(stats['errors'])}개")

        return stats


def main():
    """메인 실행 함수"""

    try:
        # 이메일 처리기 초기화
        processor = EmailProcessor()

        # 이메일 파일 경로
        email_file = "../data/email_sample.json"

        if not os.path.exists(email_file):
            logging.error(f"❌ 이메일 파일을 찾을 수 없습니다: {email_file}")
            return

        # 이메일 처리 실행
        results = processor.process_emails(email_file)

        # 결과 출력
        print("\n" + "=" * 50)
        print("📊 최종 처리 결과")
        print("=" * 50)
        print(f"총 이메일: {results['total_emails']}개")
        print(f"처리 성공: {results['processed_emails']}개")
        print(f"액션 추출: {results['actions_extracted']}개")
        print(f"오류: {len(results['errors'])}개")

        if results["errors"]:
            print("\n❌ 오류 목록:")
            for error in results["errors"]:
                print(f"  - {error}")

        print("\n✅ 처리 완료! Azure AI Search와 Table Storage에서 결과를 확인하세요.")

    except Exception as e:
        logging.error(f"❌ 메인 실행 실패: {e}")
        raise


if __name__ == "__main__":
    main()
