# Mail2DO

> 이메일 속 **‘TO-DO (Action Item)’** 을 자동으로 캐치하여 웹에서 바로 관리하는 AI MVP

<p align="left">
  <a href="#-서비스-개요--목적"><img alt="purpose" src="https://img.shields.io/badge/Purpose-Action%20from%20Email-4B7BEC"></a>
  <a href="#-기술-스택"><img alt="stack" src="https://img.shields.io/badge/Stack-Azure%20AI%20Search%20%7C%20Azure%20Functions%20%7C%20Streamlit-20C997"></a>
  <a><img alt="python" src="https://img.shields.io/badge/Python-3.10%2B-3776AB"></a>
  <a><img alt="azure" src="https://img.shields.io/badge/Azure-OpenAI%20%7C%20AI%20Search%20%7C%20Storage-0078D4"></a>
</p>

---

## 목차

* [🔎 서비스 개요 / 목적](#-서비스-개요--목적)
* [📦 프로젝트 구조](#-프로젝트-구조)
* [🧩 주요 기술 구성](#-주요-기술-구성)
* [🗺️ 아키텍처 & 데이터 플로우](#️-아키텍처--데이터-플로우)
* [🧠 기능 구현 주안점 & 핵심 로직](#-기능-구현-주안점--핵심-로직)
* [🔨 TBD](#-tbd)
* [✨ 회고](#-회고)
---

## 🔎 서비스 개요 / 목적

**Mail2DO**는 메일 본문/제목/참조 정보를 분석하여 **액션 아이템(할 일, Follow-up 등)** 을 자동으로 추출하고, 사용자가 웹 UI(스트림릿)에서 손쉽게 확인·필터링·관리할 수 있도록 돕는 AI MVP입니다.

**왜 필요한가?**

* 메일에 묻힌 업무 요청을 놓치지 않도록 **자동 가시화**
* **담당자/기한/우선순위** 등 핵심 속성을 구조화하여 **실행 가능성** 향상
* 검색 가능한 인덱스(Azure AI Search)를 기반으로 **빠른 조회** 및 **증거(원문) 추적**

> 🌏 서비스 바로가기 (Azure App Services)    
> https://webapp-slee-mail2do-dashboard.azurewebsites.net/ 

---

## 📦 프로젝트 구조

```
Mail2DO/
├── script/              # 인덱스(스키마) 정의, 인덱스 프로세서 (이메일 정규화 + 업로드)
├── functions/api/       # Azure Functions API (검색, 로그인, 액션 상태)
├── streamlit/           # Streamlit 웹 프런트엔드
├── data/                # 샘플 이메일 JSON/EML
└── README.md
```

---

## 🧩 주요 기술 구성

| 영역      | 사용 기술                          | 
| ------- | ------------------------------ | 
| 인덱싱/검색  | **Azure AI Search**            |
| 모델/추론   | **Azure OpenAI**               |
| 저장소     | **Azure Storage (Table/Blob)** |
| API     | **Azure Functions (Python)**   |
| 프런트엔드   | **Streamlit (Python)**    |

---

## 🗺️ 아키텍처 & 데이터 플로우

### 전체 아키텍처

#### 구성요소

* **Index Processor**

  * 이메일 원문 정규화(HTML→Text, 서명/광고 제거), 멘션 세그먼트 추출
  * 정책 엔진/LLM으로 액션 후보 추출 및 기한 해석(KST/UTC)
  * AI Search 인덱싱(청킹+임베딩) & Table/Blob 저장
  
* **Azure Functions API (App Layer)**

  * 로그인(Employees Table), 검색/대시보드, 상세 조회, 액션 완료 업데이트
  * 벡터+시맨틱 검색 실행

* **Streamlit Web (Presentation)**

  * 로그인, 필터·검색·정렬 UI, 상세 모달, 액션 완료 체크 → API 호출

* **Azure AI Search (Query/Index)**

  * 청크/임베딩 인덱스, 시맨틱 설정(`semantic-config`) 기반 검색/캡션

* **Azure Storage**

  * **Table**: `Employees`, `Actions` 테이블
  * **Blob**: 이메일 원문 소스 저장

* **Azure OpenAI**

  * 액션 추출용 LLM, 모호 기한 해석 보정


### 데이터 흐름 상세 설명

1. **이메일 수집 및 정규화**

   * 이메일(JSON/Azure Graph API_Outlook)을 불러와 **본문·제목·수신자/참조자 메타데이터**를 통합
   * HTML → 텍스트 변환, 광고/서명 제거

2. **인덱스 프로세싱 (Processor)**

   * 멘션 기반 세그먼트 추출: `@사용자` 단위로 action 후보(candidate) 구간 분리
   * 정책 엔진 적용: To/CC/멘션/요청 키워드 기반으로 액션 여부 판정
   * 정규식 기반 due/assignee 파싱 → LLM으로 보정

3. **Azure AI Search 업로드**

   * Azure AI Search 인덱스(`emails-index`)에 업로드

4. **Azure Storage Table (Table/Blob)**

   * 액션 JSON: Table Storage(`Actions` / `Employees`)
   * 원문 이메일/로그: Blob Storage

5. **API 계층 (Azure Functions)**

   * `/login`: `Employees` 테이블 메일 주소 인증
   * `/search`: 이메일 벡터+시맨틱 검색, 쿼리 없는 경우 최신순 조회
   * `/dashboard`: 액션 아이템 조회
   * `/action/{id}`: 액션 완료 상태 업데이트
   * `/email/{id}`: 단건 이메일 상세 조회

6. **프런트엔드 (Streamlit)**

   * 로그인 후 대시보드/검색 모드 제공
   * 클라이언트 사이드 필터링 (담당자/타입/우선순위/마감일)
   * 액션 완료 여부 체크박스 ↔ API PATCH 연동
   * 상세 다이얼로그: 본문, HTML 미리보기, 캡션 표시


---

## 🧠 기능 구현 주안점 & 핵심 로직

### 📌 인덱스 프로세서 핵심 코드

아래 코드는 인덱스 프로세서 핵심 로직입니다.    
(실제 코드에서 길이를 줄여 가독성 위주로 발췌)


##### 1) 정책 엔진(Policy Engine) — 메일 맥락에서 액션 신호 만들기

````python
def analyze_with_policy_engine(self, email_data: Dict, user_context: Dict) -> Dict:
    """정책 엔진 분석
    목적: 메일의 수신/참조 리스트와, 메일 내 멘션/요청 키워드를 기반으로 "액션 가능성" 파악 및 "정책 코드" 생성
    산출: {
      policy_decision: A|B|C|D|none, // 정책 코드
      self_sent: bool, 
      to_contains_self: bool, 
      cc_contains_self: bool,
      mentions: List[str], // 멘션 리스트
      request_detected: bool // 요청 키워드 포함 여부
    }
    """

    # 멘션 여부 파악 -> 이후 멘션 기준으로 세그먼트(action 후보 구간) 추출
    mentions = []
    if body:
        try:
            mentions = re.findall(r"@(\S+(?:\([^)]+\))?)", body)
            mentions = [f"@{mention}" for mention in mentions]
        except Exception as e:
            logging.warning(f"멘션 추출 실패: {e}")
            mentions = []

    # 요청 키워드 파악
    request_keywords = ["부탁","요청","확인","검토","승인","회신", "즉시","긴급","마감","완료","해주세요","바랍니다","처리","대응","분석","점검","실행"]
    request_detected = False
    if body:
        try:
            request_detected = any(keyword in body for keyword in request_keywords)
        except Exception as e:
            logging.warning(f"요청 키워드 감지 실패: {e}")
            request_detected = False

    # 정책 코드 설정
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
````

#### 3) LLM을 통한 액션/기한/담당자 추출
````python
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

        policy_hint = """
        [정책 코드 설명]
        - A: 수신자인 나(또는 @내이름/내가 포함된 멘션 클러스터)에게 '직접 배정'된 업무. (is_action=true)
        - B: 참조/공지(CC 등)로 '내게 직접 배정되지 않음'. @나 지목도 없음. (세그먼트 텍스트 내에 분명한 '내 배정' 근거가 없으면 is_action=false)
        - C: 내가 보낸 메일에서 타인에게 요청 (is_action=true, action['type']="FOLLOW_UP")
        - D: 팀 단위 지시(예: 백엔드개발팀)이고 내가 To에 포함되어 실제로 내 팀 일이 된 경우. (is_action=true)
        - none: 정책 판단 불가 (세그먼트 텍스트 내에 분명한 '내 배정' 근거가 없으면 is_action=false)
        """

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

            {policy_hint}

            - JSON 스키마:
            {{"is_action":true/false,"policy_decision":"A|B|C|D|none",
            "action":{{"type":"DO|FOLLOW_UP|NONE","title":"", "assignee_candidates":["이름 <이메일>","팀명"],"due_raw":null,"priority":"High|Medium|Low","tags":["태그1","태그2"],"rationale":""}}}}
            """.strip()

        user_prompt = f"""
            [세그먼트 전용 본문]
            {segment_text[:3000]}

            [세그먼트 내 기한 후보 힌트]: {deadline_hints}

            정책 신호:
            - 정책 코드: {policy_signals['policy_decision']}
            - 본인 발송: {policy_signals['self_sent']}
            - To에 본인 포함: {policy_signals['to_contains_self']}
            - 멘션: {policy_signals['mentions']}
            - 요청 감지: {policy_signals['request_detected']}

            주의: 오직 JSON 한 줄만 출력하세요.
            """.strip()

        return system_prompt, user_prompt
````
> **프롬프트 설계 포인트**
>
> * 세그먼트 스코프를 명시해 **과추출 방지**
> * '요청(배정됨)'의 케이스를 구체화해 오류 최소화
> * 출력 형식을 JSON 한 줄로 고정해 파싱 안정성 확보

#### 4) LLM 결과 교정 (후처리)

```python
def _validate_and_fix_action(self, result: dict, context: str, hints: list, policy: dict, user: dict) -> dict:
    if not isinstance(result, dict):
        return {"is_action": False, "policy_decision": "none", "action": None}
    is_action = bool(result.get("is_action")); a = result.get("action") or {}
    a_type = (a.get("type") or "NONE").upper()
    if policy.get("self_sent"):  # 내가 보낸 메일은 FOLLOW_UP 강제
        a_type, is_action = "FOLLOW_UP", True
    due_raw = (a.get("due_raw") or "").strip() or None
    if due_raw and a_type != "FOLLOW_UP":
        if not self._is_due_for_user(context, due_raw, user):
            due_raw = None  # 타인 지시로 판단 → 제거
    # title 길이 제한, tags 정규화 등 부가 정리
    # ...
    return {
        "is_action": is_action, 
        "policy_decision": policy.get("policy_decision", "none"),
        "action": {"type": a_type, "title": (a.get("title") or "")[:20].rstrip(),
        "assignee_candidates": a.get("assignee_candidates") or [],
        "due_raw": due_raw, "priority": a.get("priority", "Medium"),
        "tags": list(dict.fromkeys(a.get("tags") or [])),
        "rationale": a.get("rationale", "")}
    }
    
def _is_due_for_user(self, text: str, due_raw: str, user: dict) -> bool:
    """타인 지시 맥락일 경우 due_raw 무효화 
    -> 다른 사람에게 요청 지시한 내용에 대한 기한이 잘못 들어간 경우"""
    idx = text.find(due_raw)
    if idx == -1: return False
    mentions = list(re.finditer(MENTION_RE, text))
    # (A) 내 멘션 ~ 다음 멘션 사이
    for k, m in enumerate(mentions):
        if self._is_self_mention_text(m.group(0), user):
            seg_start = m.end(); seg_end = mentions[k+1].start() if k+1 < len(mentions) else len(text)
            if seg_start <= idx < seg_end: return True
    # (B) 멘션 클러스터 직후 기한 & 클러스터에 내가 포함되어 있으면 내 것
    # (C) 기한 앞 200자 내 마지막 멘션이 나인 경우
    # (D) 멘션이 전혀 없을 때는 완화 규칙(이름/이메일/팀 키워드, "까지/마감/요청" 패턴)
    # ... (상세 로직은 원본 코드 참조)
    return False
```


#### 5) 상대·모호 기한 해석(정규식/규칙 → LLM 보정)

```python
from datetime import datetime, timedelta, time as dt_time, timezone
from dateutil import parser
from zoneinfo import ZoneInfo
import re

def _resolve_relative_deadline(self, due_raw: str, received_at_iso: str | None) -> tuple[str | None, str | None]:
    """
    상대/모호 표현(due_raw)을 KST/UTC로 해석.
    1) 규칙 기반으로 우선 파싱(오늘/내일/이번주 X요일/EOD/EOW/YYYY-MM-DD/MM/DD/N일 후 등)
    2) 실패하면 _llm_resolve_deadline()으로 보정
    반환: ("YYYY-MM-DD HH:MM KST", "YYYY-MM-DDTHH:MM:SSZ")
    """
    if not due_raw:
        return None, None

    kst = ZoneInfo("Asia/Seoul")

    # 기준시각: 수신시각 기준, 없다면 now()로 대체
    try:
        if received_at_iso:
            base = parser.parse(received_at_iso)
            now_kst = base.astimezone(kst) if base.tzinfo else base.replace(tzinfo=kst)
        else:
            now_kst = datetime.now(kst)
    except Exception:
        now_kst = datetime.now(kst)

    # ...

    # 오전/오후 시:분
    t = re.search(r"(오전|오후)?\s*(\d{1,2})시(?:\s*(\d{1,2})분)?", text)
    if t:
        ampm, hh, mm = t.groups()
        hour = int(hh); minute = int(mm) if mm else 0
        if ampm == "오후" and hour < 12: hour += 12
        if ampm == "오전" and hour == 12: hour = 0

    # 요일 매핑
    wd_map = {"월":0, "화":1, "수":2, "목":3, "금":4, "토":5, "일":6}
    target_date = None

    # 오늘/내일/모레/금일/명일
    if re.search(r"(금일|오늘)", text):
        target_date = now_kst.date()
    elif re.search(r"(명일|내일)", text):
        target_date = (now_kst + timedelta(days=1)).date()
    elif "모레" in text:
        target_date = (now_kst + timedelta(days=2)).date()

    # 이번 주 X요일까지
    if not target_date:
        m = re.search(r"(?:이번\s*주|금주)\s*(월|화|수|목|금|토|일)요일?\s*까지?", text)
        if m:
            wd = m.group(1)
            delta = (wd_map[wd] - now_kst.weekday()) % 7
            target_date = (now_kst + timedelta(days=delta)).date()

    # 다음 주/차주 X요일까지
    if not target_date:
        m = re.search(r"(?:다음\s*주|차주)\s*(월|화|수|목|금|토|일)요일?\s*까지?", text)
        if m:
            wd = m.group(1)
            delta_to_monday = (0 - now_kst.weekday()) % 7
            next_monday = (now_kst + timedelta(days=delta_to_monday)).date() + timedelta(days=7)
            target_date = next_monday + timedelta(days=wd_map[wd])

    # YYYY-MM-DD
    if not target_date:
        m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
        if m:
            y, mo, d = map(int, m.groups())
            target_date = datetime(y, mo, d, tzinfo=kst).date()

    # N일 후/뒤
    if not target_date:
        m = re.search(r"(\d+)\s*일\s*(?:후|뒤)", text)
        if m:
            days = int(m.group(1))
            target_date = (now_kst + timedelta(days=days)).date()

    # ...

    # 규칙으로도 못 구하면 LLM 보정
    if not target_date:
        return self._llm_resolve_deadline(due_raw=text, received_at_iso=received_at_iso)

    # ...

```

> **해석 규칙 요약**
>
> * 시간 미지정 시 기본값 **18:00**
> * `오늘/금일`, `내일/명일`, `모레`, `이번 주/다음 주 X요일` 등 상대표현을 **수신시각(KST)** 기준으로 환산
> * 정형식(YYYY-MM-DD / MM/DD), 상대일수(`N일 후`) 모두 커버
> * 파싱 실패 시 LLM(`_llm_resolve_deadline`)을 통해 안전망 확보

### 실행 로그
```log
2025-09-30 13:29:07,954 - INFO - \u2705 Actions 테이블 저장 완료: API 서버 로그 분석
2025-09-30 13:29:07,954 - INFO - \U0001f4e7 처리 중: 모바일 앱 v2.1.0 배포 일정 및 QA 테스트 요청
2025-09-30 13:29:07,955 - INFO - \U0001f4cb 정책 분석: A
2025-09-30 13:29:07,955 - INFO - === \U0001f4e4 LLM 요청 (segment #1 system) ===
당신은 이메일에서 '수신자 박지훈<jihoon.park@techcorp.com>' 또는 '백엔드개발팀' 팀(그리고 박지훈이 To에 포함)에
    실제로 배정된 액션만 추출합니다. JSON 한 줄만 출력하세요(요약/설명/코드블록 금지).

    규칙:
    - 이 프롬프트는 '세그먼트' 텍스트만 제공합니다. 반드시 '세그먼트 범위 내'에서만 액션을 추출하세요.
    - '배정됨' = (내 이메일 To) 또는 (@박지훈 멘션/내가 포함된 멘션 클러스터) 또는 (팀단위 지시 + To에 내가 포함).
    - title: 12~20자, 동사+명사(예: "API 로그 분석").
    - due_raw: 원문 그대로 복사(예: "금일 오후 2시까지"). 세그먼트 밖은 절대 보지 마세요.
    - 값이 없으면 null.

    [정책 코드 설명]
        - A: 수신자인 나(또는 @내이름/내가 포함된 멘션 클러스터)에게 '직접 배정'된 업무. (is_action=true)
        - B: 참조/공지(CC 등)로 '내게 직접 배정되지 않음'. @나 지목도 없음. (세그먼트 텍스트 내에 분명한 '내 배정' 근거가 없으면 is_action=false)
        - C: 내가 보낸 메일에서 타인에게 요청 (is_action=true, action['type']="FOLLOW_UP")
        - D: 팀 단위 지시(예: 백엔드개발팀)이고 내가 To에 포함되어 실제로 내 팀 일이 된 경우. (is_action=true)
        - none: 정책 판단 불가 (세그먼트 텍스트 내에 분명한 '내 배정' 근거가 없으면 is_action=false)

    - JSON 스키마:
    {"is_action":true/false,"policy_decision":"A|B|C|D|none",
    "action":{"type":"DO|FOLLOW_UP|NONE","title":"", "assignee_candidates":["이름 <이메일>","팀명"],"due_raw":null,"priority":"High|Medium|Low","tags":["태그1","태그2"],"rationale":""}}

2025-09-30 13:29:07,955 - INFO - === \U0001f4e4 LLM 요청 (segment #1 user) ===
    [세그먼트 전용 본문]
     테스트 부탁드립니다.
    @박프로덕트(PO팀)
    배포 전 최종 승인 절차 확인 부탁드립니다.
    @박지훈(백엔드개발팀), 이번 주 금요일까지 API 연동 부분 검증 지원 부탁드립니다.
    감사합니다.
    정현우 드림
    ■ 배포 일정 QA 테스트: 9/25(월) ~ 9/27(수)
    - ■ 주요 변경사항 푸시 알림 개선
    - 

    [세그먼트 내 기한 후보 힌트]: ['이번 주 금요일까지', '9/25', '9/27']

    정책 신호:
    - 정책 코드: A
    - 본인 발송: False
    - To에 본인 포함: False
    - 멘션: ['@김테스터(QA팀)', '@박프로덕트(PO팀)', '@박지훈(백엔드개발팀),']
    - 요청 감지: True

    주의: 오직 JSON 한 줄만 출력하세요.
2025-09-30 13:29:09,016 - INFO - === \U0001f4e5 LLM 응답 (segment #1) ===
{"is_action":true,"policy_decision":"A","action":{"type":"DO","title":"API 연동 검증 지원","assignee_candidates":["박지훈 <jihoon.park@techcorp.com>","백엔드개발팀"],"due_raw":"이번 주 금요일까지","priority":"Medium","tags":["API","연동","검
증"],"rationale":"@박지훈(백엔드개발팀)이 To에 포함되어 있고, API 연동 부분 검증 지원 요청이 명확히 배정됨."}}
2025-09-30 13:29:09,016 - INFO - \u2705 세그먼트 #1 에서 액션 확정
2025-09-30 13:29:09,016 - INFO - \u26a1 최종 보정 완료: {'title': 'API 연동 검증 지원', 'assignee': '박지훈 <jihoon.park@techcorp.com>', 'due': '2025-09-26T09:00:00+00:00', 'priority': 'Medium', 'tags': ['API', '연동', '검증'], 'type'
: 'DO', 'confidence': 0.8500000000000001, 'notes': '원본 기한: 이번 주 금요일까지 | 해석(KST): 2025-09-26 18:00 KST'}
2025-09-30 13:29:09,249 - INFO - \u2705 임베딩 생성 완료: 1개
2025-09-30 13:29:09,470 - INFO - \u2705 Search 인덱스 업로드 완료: 1개 문서
2025-09-30 13:29:09,676 - INFO - \u2705 Actions 테이블 저장 완료: API 연동 검증 지원
```

---

## 🔨 TBD  (추가 예정 기능)
#### Azure Graph API(Outlook) 연동: 메일 직접 읽어오기 / 일별 데이터 수집/적재
#### Azure Entra ID 연동: 조직 계정 기반 SSO 로그인 적용
#### 임박 액션 알림 메일: 마감 임박 액션 아이템을 사용자에게 리마인더 메일 발송

---

## ✨ 회고

프로젝트를 진행하며 AI로부터 원하는 결과물을 이끌어내려면 다양한 전략과 검증 절차가 필요하다는 점을 실감했다.
단순히 모델 호출에 의존하기보다, 전처리·후처리·AI가 더 잘 이해할 법한 정책 엔진을 결합해 답변 안정성을 높이는 접근이 유효했다고 생각된다.

또한, 만들어져 있는 기능 및 플랫폼을 활용하되, 기능을 무조건 많이 쓰기보다는 기능의 용도를 이해하고 상황에 맞게 선택적으로 활용하는 것이 효율적이라는 교훈을 얻게 되었다. 예를 들어, 이메일 검색의 경우 시맨틱 검색보다는 텍스트·벡터 혼합 검색이 더 단순하면서도 효과적이지 않았을까 하는 생각을 했다.

이번 과정을 통해 직접 mvp를 만들어보면서 AI를 통해 문제 해결 방식을 어떻게 설계할 지가 더 중요해지고 있다고 느꼈다.