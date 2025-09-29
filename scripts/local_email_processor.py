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
from typing import Dict, List, Optional, Tuple
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

    def _pre_extract_deadlines(self, text: str, max_items: int = 5) -> List[str]:
        """
        본문에서 한국어 기한 표현 후보를 뽑아 LLM에 힌트로 제공.
        """
        patterns = [
            r"\(\s*\d{1,2}/\d{1,2}(?:\([^)]*\))?\s*까지\s*\)",
            r"\d{1,2}/\d{1,2}(?:\([^)]*\))?\s*까지",
            r"\d{4}-\d{1,2}-\d{1,2}(?:\s*\d{1,2}:\d{2})?\s*까지",
            r"(?:이번\s*주|금주)\s*(월|화|수|목|금|토|일)요일?\s*까지",
            r"(?:금일|오늘|내일)\s*(?:오전|오후)?\s*\d{1,2}시(?:\s*\d{1,2}분)?\s*까지",
            r"(?:오전|오후)?\s*\d{1,2}시(?:\s*\d{1,2}분)?\s*까지",
            r"(?:금일|오늘|내일)\s*까지",
            # ▼ '까지' 없는 흔한 표현
            r"마감[:\s]*\d{1,2}/\d{1,2}(?:\([^)]*\))?",
            r"\d{1,2}/\d{1,2}(?:\([^)]*\))?(?:\s*\d{1,2}:\d{2})?",
            r"\d{4}-\d{1,2}-\d{1,2}(?:\s*\d{1,2}:\d{2})?",
            r"\d+\s*일\s*(?:후|뒤)",
            r"\b(?:EOD|EOW)\b",
            r"(업무\s*(?:종료|시간)\s*전)",
            r"\d{1,2}/\d{1,2}\s*~\s*\d{1,2}/\d{1,2}",
            r"\d{4}-\d{1,2}-\d{1,2}\s*~\s*\d{4}-\d{1,2}-\d{1,2}",
            # ▼ 주/월 내
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

    def _sanitize_document_key(self, key: str) -> str:
        """Azure Search 문서 키 정제"""

        # Azure Search 키 규칙: 문자, 숫자, 언더스코어(_), 대시(-), 등호(=)만 허용
        # 특수문자를 언더스코어로 변경
        sanitized = re.sub(r"[^a-zA-Z0-9_\-=]", "_", key)

        # 연속된 언더스코어 정리
        sanitized = re.sub(r"_+", "_", sanitized)

        # 시작/끝 언더스코어 제거
        sanitized = sanitized.strip("_")

        # 길이 제한 (Azure Search 키 최대 길이: 1024자)
        if len(sanitized) > 1000:
            # 해시를 사용하여 고유성 보장
            hash_suffix = hashlib.md5(key.encode()).hexdigest()[:8]
            sanitized = sanitized[:992] + "_" + hash_suffix

        return sanitized

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

    def _html_to_text(self, html_str: str) -> str:
        if not html_str:
            return ""
        # 아주 가벼운 변환(BeautifulSoup 없이)
        text = re.sub(
            r"(?is)<(script|style).*?>.*?</\1>", " ", html_str
        )  # 스크립트/스타일 제거
        text = re.sub(r"(?is)<br\s*/?>", "\n", text)
        text = re.sub(r"(?is)</p>", "\n", text)
        text = re.sub(r"(?is)</li>", "\n- ", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)  # 태그 제거
        text = html.unescape(text)
        text = re.sub(r"[ \t\u00A0]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

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

        def _html_to_text(self, html_str: str) -> str:
            if not html_str:
                return ""
            import html as _html

            text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html_str)
            text = re.sub(r"(?is)<br\s*/?>", "\n", text)
            text = re.sub(r"(?is)</p>", "\n", text)
            text = re.sub(r"(?is)</li>", "\n- ", text)
            text = re.sub(r"(?is)<[^>]+>", " ", text)
            text = _html.unescape(text)
            text = re.sub(r"[ \t\u00A0]+", " ", text)
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text.strip()

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

        # 안전한 배열 처리
        to_names = safe_get_list("to_names")
        to_addresses = safe_get_list("to_addresses")
        cc_names = safe_get_list("cc_names")
        cc_addresses = safe_get_list("cc_addresses")

        # 길이 맞추기 (names와 addresses 배열 길이가 다를 수 있음)
        max_to_len = max(len(to_names), len(to_addresses))
        max_cc_len = max(len(cc_names), len(cc_addresses))

        # to 리스트 정규화
        to_list = []
        for i in range(max_to_len):
            name = to_names[i] if i < len(to_names) else ""
            email = to_addresses[i] if i < len(to_addresses) else ""
            if name or email:  # 적어도 하나는 있어야 함
                to_list.append({"name": name, "email": email})

        # cc 리스트 정규화
        cc_list = []
        for i in range(max_cc_len):
            name = cc_names[i] if i < len(cc_names) else ""
            email = cc_addresses[i] if i < len(cc_addresses) else ""
            if name or email:  # 적어도 하나는 있어야 함
                cc_list.append({"name": name, "email": email})

        # threads 데이터 안전한 처리
        threads = email_data.get("threads", {})
        keywords = []
        if isinstance(threads, dict):
            keywords = threads.get("keywords", [])
            if not isinstance(keywords, list):
                keywords = []

        # 표준화된 형태로 변환
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

    def analyze_with_policy_engine(self, email_data: Dict, user_context: Dict) -> Dict:
        """정책 엔진 분석 (안전한 처리)"""

        # 안전한 필드 추출
        from_email = email_data.get("from_address", "")
        to_emails = email_data.get("to_addresses", [])
        cc_emails = email_data.get("cc_addresses", [])
        body = email_data.get("email_body", "")

        # None 체크 및 기본값 설정
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

        # 멘션 추출 (빈 문자열 체크)
        mentions = []
        if body:
            try:
                mentions = re.findall(r"@(\S+(?:\([^)]+\))?)", body)
                mentions = [f"@{mention}" for mention in mentions]
            except Exception as e:
                logging.warning(f"멘션 추출 실패: {e}")
                mentions = []

        # 요청 키워드 감지
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

        # 기본 판정 요소 (안전한 비교)
        self_sent = bool(from_email and user_email and from_email == user_email)
        to_contains_self = bool(user_email and user_email in to_emails)
        cc_contains_self = bool(user_email and user_email in cc_emails)

        # 정책 결정
        policy_decision = "none"

        try:
            if to_contains_self and request_detected:
                # A: To에 포함 + 요청 표현
                user_mentions = [
                    mention
                    for mention in mentions
                    if user_name and f"@{user_name}" in mention
                ]
                if (
                    not any(
                        mention for mention in mentions if mention != f"@{user_name}"
                    )
                    or user_mentions
                ):
                    policy_decision = "A"
            elif cc_contains_self and not to_contains_self:
                # B: CC에만 포함
                if any(
                    user_name and f"@{user_name}" in mention for mention in mentions
                ):
                    policy_decision = "A"  # 명시적 지목이면 액션
                else:
                    policy_decision = "B"  # 비액션
            elif self_sent and request_detected:
                # C: 본인이 보낸 요청
                policy_decision = "C"
            elif (
                to_contains_self
                and user_team
                and user_team in body
                and request_detected
            ):
                # D: 팀 단위 요청
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

    def extract_actions_with_llm(
        self, email_data: Dict, policy_signals: Dict, user_context: Dict
    ) -> Dict:
        """LLM을 사용한 액션 추출"""

        schema_block = """
        다음 JSON 형식으로만, 한 줄의 유효한 JSON으로 응답하세요:
        {
        "is_action": true,
        "policy_decision": "A|B|C|D|none",
        "action": {
            "type": "DO|FOLLOW_UP|NONE",
            "title": "액션 제목(20자 이내)",
            "assignee_candidates": ["이름 <이메일>", "팀명"],
            "due_raw": "원본 기한 표현(힌트에서 고르거나 본문에서 그대로 발췌; 없으면 null)",
            "priority": "High|Medium|Low",
            "tags": ["태그1", "태그2"],
            "rationale": "판단 근거(1~2문장)"
        }
        }
        주의:
        - 무조건 JSON만 출력(코드블록, 설명 금지)
        - 값이 없으면 null 로 채워라
        - due_raw 는 반드시 '원문 표현'을 그대로 복사
        """.strip()

        system_prompt = f"""
            당신은 이메일에서 액션 아이템을 추출하는 전문가입니다.

            다음 정책 규칙을 반드시 준수하세요:
            - A: To에 포함 + 요청 표현 → DO 액션
            - B: CC에만 포함 + 명시적 지목 없음 → 비액션
            - C: 본인이 보낸 요청 → FOLLOW_UP 액션
            - D: 팀 단위 요청 + To에 포함 → DO 액션

            사용자 정보:
            - 이름: {user_context['name']}
            - 이메일: {user_context['email']}
            - 팀: {user_context['team']}

            JSON 형식으로만 응답하세요.
            """.strip()

        deadline_hints = self._collect_deadline_hints(email_data)

        user_prompt = f"""
            이메일 분석:

            제목: {email_data['subject']}
            발신자: {email_data['from']['name']} <{email_data['from']['email']}>
            수신자: {', '.join([f"{p['name']} <{p['email']}>" for p in email_data['to']])}
            참조: {', '.join([f"{p['name']} <{p['email']}>" for p in email_data['cc']])}
            날짜: {email_data['receivedAt']}

            본문:
            {email_data['body'][:3000]}

            [기한 후보 힌트] 본문에서 규칙 기반으로 미리 탐지된 표현들:
            {deadline_hints}

            정책 신호:
            - 정책 결정: {policy_signals['policy_decision']}
            - 본인 발송: {policy_signals['self_sent']}
            - To에 본인 포함: {policy_signals['to_contains_self']}
            - 멘션: {policy_signals['mentions']}
            - 요청 감지: {policy_signals['request_detected']}
            - title은 12~20자 한국어 문장으로, 나에게 할당된 핵심 작업을 동사+명사로 요약(예: "API 서버 로그 분석").
            - [기한 후보 힌트]가 비어 있어도, 본문/제목에서 직접 날짜·요일·시간·범위를 찾아 due_raw에 '원문 그대로' 복사해라. 정말 원문에 아무 표현도 없을 때만 null을 사용한다.
            
            {schema_block}
            """.strip()

        try:
            logging.info("=== 📤 LLM 요청 (system) ===\n%s", system_prompt)
            logging.info("=== 📤 LLM 요청 (user) ===\n%s", user_prompt)

            response = self.openai_client.chat.completions.create(
                model=self.azure_openai_deployment_chat,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=800,
            )

            response_text = response.choices[0].message.content.strip()
            logging.info("=== 📥 LLM 원본 응답 ===\n%s", response_text)

            result = json.loads(response_text)
            logging.info("✅ LLM 액션 추출 완료: %s", result.get("is_action", False))
            return result

        except json.JSONDecodeError as e:
            logging.error("❌ JSON 파싱 실패: %s", e)
            logging.error(
                "LLM 원본 응답:\n%s",
                response_text if "response_text" in locals() else "N/A",
            )
            return {"is_action": False, "policy_decision": "none", "action": None}
        except Exception as e:
            logging.exception("❌ LLM 추출 실패")
            return {"is_action": False, "policy_decision": "none", "action": None}

    def normalize_action(self, raw_action: Dict, email_data: Dict) -> Optional[Dict]:
        """액션 데이터 정규화 (KST 기준 해석 → UTC ISO 저장, 시간/요일/상대일 지원)"""

        if not raw_action.get("is_action") or not raw_action.get("action"):
            return None

        action = raw_action["action"]
        due_raw = (action.get("due_raw") or "").strip()

        # 담당자 결정
        assignee = "미지정"
        for cand in action.get("assignee_candidates") or []:
            if "@" in cand:
                assignee = cand
                break

        # 기본 신뢰도
        confidence = self.default_confidence

        # === 날짜/시간 파싱 ===
        due_iso = None
        if due_raw:
            try:
                kst = ZoneInfo("Asia/Seoul")
                now_kst = datetime.now(kst)

                # 시간/분 추출
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

                # 상대일
                if re.search(r"(금일|오늘)", due_raw):
                    target_date = now_kst.date()
                elif "내일" in due_raw:
                    target_date = (now_kst + timedelta(days=1)).date()
                # 이번주 요일까지
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
                # YYYY-MM-DD
                elif re.search(r"\d{4}-\d{1,2}-\d{1,2}", due_raw):
                    y, m, d = map(
                        int, re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", due_raw).groups()
                    )
                    target_date = datetime(y, m, d, tzinfo=kst).date()
                # MM/DD
                elif re.search(r"\d{1,2}/\d{1,2}", due_raw):
                    m, d = map(int, re.search(r"(\d{1,2})/(\d{1,2})", due_raw).groups())
                    y = now_kst.year if m >= now_kst.month else now_kst.year + 1
                    target_date = datetime(y, m, d, tzinfo=kst).date()
                # N일 후/뒤
                elif re.search(r"\d+\s*일\s*(?:후|뒤)", due_raw):
                    days = int(re.search(r"(\d+)\s*일\s*(?:후|뒤)", due_raw).group(1))
                    target_date = (now_kst + timedelta(days=days)).date()

                # 마지막 수단: dateutil
                if not target_date:
                    try:
                        parsed = parser.parse(due_raw, fuzzy=True)
                        parsed = (
                            parsed.replace(tzinfo=kst)
                            if not parsed.tzinfo
                            else parsed.astimezone(kst)
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
            except Exception as e:
                logging.error(f"날짜/시간 정규화 오류: {e}, due_raw: {due_raw}")
                due_iso = None

        # 신뢰도 보정
        if action.get("type") == "DO" and due_iso and "@" in assignee:
            confidence = min(confidence + 0.2, 1.0)
        elif action.get("type") == "FOLLOW_UP" and due_iso:
            confidence = min(confidence + 0.15, 1.0)

        return {
            "title": action.get("title", ""),
            "assignee": assignee,
            "due": due_iso,  # ISO(UTC) 문자열 또는 None
            "priority": action.get("priority", "Medium"),
            "tags": action.get("tags", []),
            "type": action.get("type", "DO"),
            "confidence": confidence,
            "notes": f"원본 기한: {due_raw}" if due_raw else "",
        }

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
                # action_data['due']가 ISO(UTC)일 수도(None일 수도) 있으니 그대로 사용
                document.update(
                    {
                        "action_type": action_data.get("type", ""),
                        "assignee": action_data.get("assignee", ""),
                        "due": action_data.get("due"),  # 더 이상 T00:00:00Z 붙이지 않음
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
                "due": action_data.get("due", ""),
                "priority": action_data.get("priority", ""),
                "type": action_data.get("type", ""),
                "tags": ";".join(action_data.get("tags", [])),
                "confidence": action_data.get("confidence", 0.0),
                "receivedAt": email_data["receivedAt"],
                "conversationId": email_data.get("conversationId", ""),
                "webLink": "",
            }

            actions_table.upsert_entity(entity)
            logging.info(f"✅ Actions 테이블 저장 완료: {action_data['title']}")

        except Exception as e:
            logging.error(f"❌ Actions 테이블 저장 실패: {e}")

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

                # 3. 정책 엔진 적용
                policy_signals = self.analyze_with_policy_engine(
                    email_data, user_context
                )
                logging.info(f"📋 정책 분석: {policy_signals['policy_decision']}")

                # 4. LLM 액션 추출
                action_result = self.extract_actions_with_llm(
                    standardized_email, policy_signals, user_context
                )

                # 5. 액션 정규화
                normalized_action = None
                if action_result.get("is_action"):
                    normalized_action = self.normalize_action(
                        action_result, standardized_email
                    )
                    if normalized_action:
                        stats["actions_extracted"] += 1
                        logging.info(f"⚡ 액션 추출: {normalized_action['title']}")

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
