# streamlit/app.py
import streamlit as st
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from dotenv import load_dotenv
import re
import os

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL")
API_FUNCTION_KEY = os.getenv("API_FUNCTION_KEY")

session = requests.Session()
session.headers.update(
    {"x-functions-key": API_FUNCTION_KEY, "Content-Type": "application/json"}
)

# 페이지 설정
st.set_page_config(
    page_title="Mail2DO",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# CSS 스타일
st.markdown(
    """
<style>
    .main-header {
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
    }
    .action-row {
        background: white;
        padding: 0.8rem;
        border-radius: 8px;
        border-left: 4px solid #1f77b4;
        margin-bottom: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    .priority-high { border-left-color: #d32f2f; }
    .priority-medium { border-left-color: #ff9800; }
    .priority-low { border-left-color: #4caf50; }

    .metric-container {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        text-align: center;
        margin-bottom: 1rem;
    }
</style>
""",
    unsafe_allow_html=True,
)


class EmailDashboard:
    """이메일 대시보드 메인 클래스"""

    def __init__(self):
        self.api_base_url = API_BASE_URL

    def authenticate_user(self, email: str) -> Optional[Dict]:
        """사용자 인증"""
        try:
            response = session.post(
                f"{self.api_base_url}/login", json={"email": email}, timeout=10
            )

            if response.status_code == 200:
                return response.json()
            else:
                error_data = response.json()
                st.error(f"인증 실패: {error_data.get('error', '알 수 없는 오류')}")
                return None

        except Exception as e:
            st.error(f"인증 중 오류 발생: {e}")
            return None

    def search_emails(self, query: str, user_email: str) -> List[Dict]:
        """
        이메일 검색 (필터링 없이)
        - 검색 쿼리만 API로 전송
        - 필터링은 클라이언트에서 수행
        """
        try:
            payload = {"query": query, "user_email": user_email}

            response = session.post(
                f"{self.api_base_url}/search", json=payload, timeout=30
            )

            if response.status_code == 200:
                return response.json().get("results", [])
            else:
                error_data = response.json()
                st.error(f"검색 실패: {error_data.get('error', '알 수 없는 오류')}")
                return []

        except Exception as e:
            st.error(f"검색 중 오류 발생: {e}")
            return []

    def get_dashboard_data(self, user_email: str) -> List[Dict]:
        """대시보드 데이터 조회 (필터링 없이)"""
        try:
            response = session.post(
                f"{self.api_base_url}/dashboard",
                json={"user_email": user_email},
                timeout=30,
            )

            if response.status_code == 200:
                return response.json().get("items", [])
            else:
                error_data = response.json()
                st.error(
                    f"대시보드 로딩 실패: {error_data.get('error', '알 수 없는 오류')}"
                )
                return []

        except Exception as e:
            st.error(f"대시보드 로딩 중 오류 발생: {e}")
            return []

    def get_email_detail(self, email_id: str) -> Optional[Dict]:
        """이메일 상세 정보 조회"""
        try:
            response = session.get(f"{self.api_base_url}/email/{email_id}", timeout=15)

            if response.status_code == 200:
                return response.json()
            else:
                error_data = response.json()
                st.error(
                    f"이메일 조회 실패: {error_data.get('error', '알 수 없는 오류')}"
                )
                return None

        except Exception as e:
            st.error(f"이메일 조회 중 오류 발생: {e}")
            return None

    def update_action_status(self, action_id: str, done: bool) -> bool:
        """액션 완료 상태 업데이트"""
        try:
            response = session.patch(
                f"{self.api_base_url}/action/{action_id}",
                json={"done": done},
                timeout=10,
            )

            if response.status_code == 200:
                return True
            else:
                error_data = response.json()
                st.error(
                    f"상태 업데이트 실패: {error_data.get('error', '알 수 없는 오류')}"
                )
                return False

        except Exception as e:
            st.error(f"상태 업데이트 중 오류 발생: {e}")
            return False


def apply_client_side_filters(
    items: List[Dict],
    user_email: str,
    assignee_filter: str,
    action_types: List[str],
    priorities: List[str],
    completion_filter: str,
    due_date_filter: Optional[str] = None,
) -> List[Dict]:
    """
    클라이언트 사이드 필터링 로직

    Args:
        items: 필터링할 아이템 목록
        user_email: 현재 사용자 이메일
        assignee_filter: 담당자 필터 ("me", "all", "unassigned")
        action_types: 액션 타입 필터 리스트 (["DO", "FOLLOW_UP"])
        priorities: 우선순위 필터 리스트 (["High", "Medium", "Low"])
        completion_filter: 완료 상태 필터 ("incomplete", "complete", "all")
        due_date_filter: 마감일 필터 ("today", "week", "month", "overdue", None)

    Returns:
        필터링된 아이템 목록
    """
    if not items:
        return []

    filtered_items = []
    user_email_lower = user_email.lower()
    today = datetime.now().date()

    for item in items:
        try:
            # 1. 담당자 필터링
            assignee_email = (item.get("assignee_email") or "").lower()
            assignee_display = item.get("assignee") or ""

            if assignee_filter == "me":
                # "나"로 필터링: 현재 사용자 이메일이 포함된 항목만
                if user_email_lower not in assignee_email:
                    continue
            elif assignee_filter == "unassigned":
                # "미지정"으로 필터링
                if assignee_display != "미지정":
                    continue
            # assignee_filter == "all"인 경우 모든 항목 포함

            # 2. 액션 타입 필터링
            if action_types:
                item_action_type = item.get("actionType", "DO")
                if item_action_type not in action_types:
                    continue

            # 3. 우선순위 필터링
            if priorities:
                item_priority = item.get("priority", "Medium")
                if item_priority not in priorities:
                    continue

            # 4. 완료 상태 필터링
            is_done = item.get("done", False)
            if completion_filter == "incomplete" and is_done:
                continue
            elif completion_filter == "complete" and not is_done:
                continue
            # completion_filter == "all"인 경우 모든 항목 포함

            # 5. 마감일 필터링
            if due_date_filter:
                due_str = item.get("due")

                if not due_str:
                    # 마감일이 없는 경우: due_date_filter가 None이 아니면 제외
                    continue

                try:
                    # ISO 형식 날짜 파싱
                    due_date = datetime.fromisoformat(
                        due_str.replace("Z", "+00:00")
                    ).date()

                    if due_date_filter == "today":
                        # 오늘 마감
                        if due_date != today:
                            continue
                    elif due_date_filter == "week":
                        # 이번 주 마감 (오늘 포함 7일)
                        week_end = today + timedelta(days=7)
                        if not (today <= due_date <= week_end):
                            continue
                    elif due_date_filter == "month":
                        # 이번 달 마감 (오늘 포함 30일)
                        month_end = today + timedelta(days=30)
                        if not (today <= due_date <= month_end):
                            continue
                    elif due_date_filter == "overdue":
                        # 기한 초과 (오늘 이전)
                        if due_date >= today:
                            continue
                except Exception:
                    # 날짜 파싱 실패 시 해당 항목 제외
                    continue

            # 모든 필터를 통과한 항목 추가
            filtered_items.append(item)

        except Exception as e:
            # 개별 아이템 처리 실패 시 계속 진행
            continue

    return filtered_items


def render_login_page():
    """로그인 페이지 렌더링"""
    st.markdown(
        '<div class="main-header"><h1>📧 Mail2DO</h1><h5>이메일에서 액션으로, 자동 추출 & 관리</h5></div>',
        unsafe_allow_html=True,
    )

    st.markdown("### 🔐 로그인")

    with st.form("login_form"):
        email = st.text_input(
            "이메일 주소",
            placeholder="example@techcorp.com",
            help="회사 이메일 주소를 입력하세요",
        )
        submitted = st.form_submit_button("로그인", use_container_width=True)

        if submitted and email:
            dashboard = EmailDashboard()
            user_info = dashboard.authenticate_user(email)

            if user_info:
                st.session_state.user_info = user_info
                st.success(f"환영합니다, {user_info['name']}님!")
                st.rerun()

    # 샘플 계정 정보 표시
    st.info(
        """
    **샘플 계정**
    - jihoon.park@techcorp.com (박지훈 - 백엔드개발팀)
    """
    )


def render_dashboard_page():
    """대시보드 메인 페이지 렌더링"""
    user_info = st.session_state.user_info
    dashboard = EmailDashboard()

    # 헤더
    col1, col2 = st.columns([5, 1])
    with col1:
        st.markdown(
            f'<div class="main-header"><h1>📧 안녕하세요. {user_info["name"]}님</h1></div>',
            unsafe_allow_html=True,
        )
    with col2:
        if st.button("🚪 로그아웃", use_container_width=True):
            del st.session_state.user_info
            # 세션 상태 초기화
            if "search_triggered" in st.session_state:
                del st.session_state.search_triggered
            st.rerun()

    # 사이드바 필터
    with st.sidebar:
        st.markdown("### 🔍 검색 및 필터")
        search_query = st.text_input("검색어", placeholder="키워드를 입력하세요...")

        st.markdown("#### 담당자")
        assignee_filter = st.selectbox(
            "담당자 필터",
            ["me", "all", "unassigned"],
            format_func=lambda x: {"me": "나", "all": "전체", "unassigned": "미지정"}[
                x
            ],
            index=0,
        )

        st.markdown("#### 액션 타입")
        action_types = st.multiselect(
            "액션 타입",
            ["DO", "FOLLOW_UP"],
            default=["DO"],
            format_func=lambda x: {"DO": "할 일", "FOLLOW_UP": "추적"}[x],
        )

        st.markdown("#### 우선순위")
        priorities = st.multiselect(
            "우선순위",
            ["High", "Medium", "Low"],
            default=[],
            format_func=lambda x: {"High": "높음", "Medium": "보통", "Low": "낮음"}[x],
        )

        st.markdown("#### 완료 상태")
        completion_filter = st.selectbox(
            "완료 상태 필터",
            ["incomplete", "complete", "all"],
            format_func=lambda x: {
                "incomplete": "미완료",
                "complete": "완료",
                "all": "전체",
            }[x],
            index=0,
        )

        st.markdown("#### 마감일")
        due_date_filter = st.selectbox(
            "마감일 필터",
            [None, "today", "week", "month", "overdue"],
            format_func=lambda x: {
                None: "전체",
                "today": "오늘",
                "week": "이번 주",
                "month": "이번 달",
                "overdue": "기한 초과",
            }[x],
            index=0,
        )

        if st.button("🔍 검색", use_container_width=True):
            st.session_state.search_triggered = True

    # 메인 콘텐츠 영역
    if search_query or st.session_state.get("search_triggered"):
        # 검색 모드
        with st.spinner("검색 중..."):
            # API에서 검색 결과 가져오기 (필터링 없이)
            results = dashboard.search_emails(search_query, user_info["email"])

        # 클라이언트 사이드 필터링 적용
        filtered_results = apply_client_side_filters(
            items=results,
            user_email=user_info["email"],
            assignee_filter=assignee_filter,
            action_types=action_types,
            priorities=priorities,
            completion_filter=completion_filter,
            due_date_filter=due_date_filter,
        )

        st.markdown(f"### 🔍 검색 결과 ({len(filtered_results)}개)")

        # 결과 정렬: 우선순위 > 마감일
        if filtered_results:
            priority_order = {"High": 0, "Medium": 1, "Low": 2}
            filtered_results = sorted(
                filtered_results,
                key=lambda x: (
                    priority_order.get(x.get("priority", "Medium"), 1),
                    x.get("due") or "9999-12-31",
                ),
            )

        render_email_results_with_checkbox(filtered_results, dashboard)

    else:
        # 대시보드 모드
        with st.spinner("대시보드 로딩 중..."):
            # API에서 모든 데이터 가져오기 (필터링 없이)
            dashboard_items = dashboard.get_dashboard_data(user_info["email"])

        # 클라이언트 사이드 필터링 적용
        filtered_items = apply_client_side_filters(
            items=dashboard_items,
            user_email=user_info["email"],
            assignee_filter=assignee_filter,
            action_types=action_types,
            priorities=priorities,
            completion_filter=completion_filter,
            due_date_filter=due_date_filter,
        )

        # 정렬: 우선순위 > 마감일
        if filtered_items:
            priority_order = {"High": 0, "Medium": 1, "Low": 2}
            filtered_items = sorted(
                filtered_items,
                key=lambda x: (
                    priority_order.get(x.get("priority", "Medium"), 1),
                    x.get("due") or "9999-12-31",
                ),
            )

        # 메트릭 표시 (필터링된 데이터 기준)
        render_dashboard_metrics(filtered_items)

        # 액션 아이템 목록
        st.markdown("### ✔ 나의 액션 아이템")
        render_email_results_with_checkbox(filtered_items, dashboard)


def render_dashboard_metrics(items: List[Dict]):
    """
    대시보드 상단의 메트릭 표시

    Args:
        items: 필터링된 아이템 목록
    """
    if not items:
        st.info("필터 조건에 맞는 액션이 없습니다.")
        return

    # 메트릭 계산
    total_items = len(items)
    high_priority = len([item for item in items if item.get("priority") == "High"])

    # due_today 계산 시 None 처리
    today_iso = datetime.now().date().isoformat()
    due_today = len(
        [
            item
            for item in items
            if item.get("due") and item.get("due").startswith(today_iso)
        ]
    )

    # 메트릭 표시
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f"""
        <div class="metric-container">
            <h4 style="color: #1f77b4; margin: 0;">{total_items}</h4>
            <p style="margin: 0;">전체 액션</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
        <div class="metric-container">
            <h4 style="color: #d32f2f; margin: 0;">{high_priority}</h4>
            <p style="margin: 0;">높은 우선순위</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            f"""
        <div class="metric-container">
            <h4 style="color: #ff9800; margin: 0;">{due_today}</h4>
            <p style="margin: 0;">오늘 마감</p>
        </div>
        """,
            unsafe_allow_html=True,
        )


def render_email_results_with_checkbox(results: List[Dict], dashboard: EmailDashboard):
    """체크박스가 있는 이메일 결과 목록 렌더링"""
    if not results:
        st.info("🔍 조건에 맞는 결과가 없습니다.")
        return

    for i, item in enumerate(results):
        priority = item.get("priority", "Medium")
        item_id = item.get("id")
        is_done = item.get("done", False)

        # 마감일 포맷팅
        due_formatted = format_due_date_detail(item.get("due"))
        assignee = item.get("assignee", "미지정")
        action_type = item.get("actionType", "DO")
        action_type_kr = "할 일" if action_type == "DO" else "추적"
        subject = item.get("subject", "No Subject")

        # 우선순위별 이모지
        priority_emoji = (
            "🔴" if priority == "High" else "🟠" if priority == "Medium" else "🟢"
        )

        # 행 컨테이너
        col_check, col_content, col_button = st.columns([0.5, 8.5, 1])

        with col_check:
            # 체크박스: 고유한 키 생성
            checkbox_key = f"check_{item_id}_{i}"
            checkbox_value = st.checkbox(
                "",
                value=is_done,
                key=checkbox_key,
                label_visibility="collapsed",
            )

            # 체크박스 상태가 변경되면 API 호출
            if checkbox_value != is_done:
                if dashboard.update_action_status(item_id, checkbox_value):
                    # 세션 상태에 토스트 메시지 저장
                    status_text = "완료" if checkbox_value else "미완료"
                    st.session_state.toast_message = (
                        f"액션이 {status_text}로 변경되었습니다"
                    )
                    st.rerun()

        with col_content:
            # 제목 줄
            title_col1, title_col2 = st.columns([8, 2])
            with title_col1:
                if is_done:
                    st.markdown(f"~~**{subject}**~~")
                else:
                    st.markdown(f"**{subject}**")
            with title_col2:
                badge_text = f"{priority_emoji} {priority}"
                if is_done:
                    badge_text = "✅ " + badge_text
                st.markdown(f"`{badge_text}`")

            # 상세 정보
            if is_done:
                st.caption(f"~~{action_type_kr} · @{assignee} · ~ {due_formatted}~~")
            else:
                st.caption(f"{action_type_kr} · @{assignee} · ~ {due_formatted}")

        with col_button:
            # 상세 보기 버튼: 고유한 키 생성
            detail_key = f"detail_{item_id}_{i}"
            if st.button("📄", key=detail_key, help="자세히 보기"):
                show_detail_dialog(item, dashboard)

        st.divider()


@st.dialog("📧 액션 상세 정보", width="large")
def show_detail_dialog(item: Dict, dashboard: EmailDashboard):
    """상세 정보 다이얼로그"""
    email_id = item.get("emailId")

    # 기본 정보 섹션
    priority = item.get("priority", "Medium")
    priority_color = {
        "High": "#d32f2f",
        "Medium": "#ff9800",
        "Low": "#4caf50",
    }.get(priority, "#ff9800")

    st.markdown(
        f"""
    <div style="background: {priority_color}; color: white; padding: 1rem; border-radius: 8px; margin-bottom: 1rem;">
        <h3 style="margin: 0; font-size: 1.2rem;">[{item.get('actionType', 'DO')}] {item.get('action', 'No Action')}</h3>
        <h5 style="margin: 0; font-size: 1rem;">{item.get('subject', 'No Subject')}</h5>
        <p style="margin: 0.5rem 0 0 0; font-size: 0.9rem;">
            우선순위: {priority} | 상태: {'완료' if item.get('done', False) else '미완료'}
        </p>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # 메타 정보
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 📋 액션 정보")
        st.markdown(f"**액션 타입:** {item.get('actionType', 'DO')}")
        st.markdown(f"**담당자:** @{item.get('assignee', '미지정')}")
        st.markdown(f"**마감일:** {format_due_date_detail(item.get('due'))}")

    with col2:
        st.markdown("#### 📨 이메일 정보")
        st.markdown(f"**메일명:** {item.get('subject', 'No Subject')}")
        st.markdown(f"**발신자:** {item.get('from_name', 'Unknown')}")
        to_names = item.get("to_names", [])
        first_recipient = to_names[0] if to_names else "Unknown"
        st.markdown(f"**수신자:** {first_recipient}")

    # 태그
    if item.get("tags"):
        st.markdown("#### 🏷️ 태그")
        tags_html = " ".join(
            [
                f'<span style="background: #e3f2fd; color: #1976d2; padding: 0.3rem 0.6rem; border-radius: 8px; font-size: 0.85rem; margin-right: 0.5rem;">#{tag}</span>'
                for tag in item.get("tags", [])
            ]
        )
        st.markdown(tags_html, unsafe_allow_html=True)

    st.divider()

    # 본문 미리보기
    st.markdown("#### 📝 본문 미리보기")
    st.text_area(
        "",
        item.get("bodyPreview", "내용 없음"),
        height=150,
        disabled=True,
        label_visibility="collapsed",
    )

    # 상세 정보 로드
    if email_id:
        with st.spinner("상세 정보 로딩 중..."):
            email_detail = dashboard.get_email_detail(email_id)

        if email_detail:
            st.divider()

            # 전체 이메일 정보 탭
            tab1, tab2, tab3 = st.tabs(
                ["📧 전체 본문", "👥 수신자 상세", "🌐 HTML 미리보기"]
            )

            received_at = email_detail.get("receivedAt", "")
            received_at_kst = format_received_date_kst(received_at)

            with tab1:
                st.text_area(
                    "전체 본문",
                    email_detail.get("full_body", ""),
                    height=300,
                    disabled=True,
                    label_visibility="collapsed",
                )

            with tab2:
                st.markdown(
                    f"**발신자:** {email_detail.get('from_name', '')} <{email_detail.get('from_email', '')}>"
                )
                to_names_list = email_detail.get("to_names", [])
                if to_names_list:
                    st.markdown(f"**수신자:** {', '.join(to_names_list)}")
                else:
                    st.markdown("**수신자:** 없음")

                cc_names_list = email_detail.get("cc_names", [])
                if cc_names_list:
                    st.markdown(f"**참조:** {', '.join(cc_names_list)}")

                st.markdown(f"**날짜:** {received_at_kst}")

            with tab3:
                html_body = email_detail.get("html_body")
                if html_body:
                    # 보안을 위한 기본적인 HTML 정제
                    html_content = re.sub(
                        r"<script.*?</script>",
                        "",
                        html_body,
                        flags=re.DOTALL | re.IGNORECASE,
                    )
                    html_content = re.sub(
                        r'href\s*=\s*["\']https?://[^"\']*["\']',
                        'href="#"',
                        html_content,
                        flags=re.IGNORECASE,
                    )

                    st.components.v1.html(html_content, height=400, scrolling=True)
                else:
                    st.info("HTML 내용이 없습니다.")

    # 시맨틱 캡션 (검색 결과인 경우)
    captions = item.get("captions", [])
    if captions:
        st.divider()
        st.markdown("#### 🎯 관련 부분")
        for caption in captions[:3]:
            highlight_text = caption.get("highlights", caption.get("text", ""))
            if highlight_text:
                st.info(highlight_text)


def format_due_date_detail(due_str: Optional[str]) -> str:
    """
    마감일 상세 포맷팅 (YYYY.MM.DD HH:MM)

    Args:
        due_str: ISO 형식 날짜 문자열

    Returns:
        포맷된 날짜 문자열
    """
    if not due_str:
        return "미지정"

    try:
        due_datetime = datetime.fromisoformat(due_str.replace("Z", "+00:00"))

        # UTC를 KST(+9시간)로 변환
        kst = timezone(timedelta(hours=9))
        due_datetime_kst = due_datetime.astimezone(kst)

        return due_datetime_kst.strftime("%Y.%m.%d %H:%M")
    except Exception:
        return due_str


def format_received_date_kst(date_str: str) -> str:
    """ISO -> KST 시간으로 포맷팅"""
    if not date_str:
        return "날짜 정보 없음"

    try:
        # ISO 형식 파싱
        received_datetime = datetime.fromisoformat(date_str.replace("Z", "+00:00"))

        # UTC를 KST(+9시간)로 변환
        kst = timezone(timedelta(hours=9))
        received_datetime_kst = received_datetime.astimezone(kst)

        return received_datetime_kst.strftime("%Y년 %m월 %d일 %H:%M:%S (KST)")
    except Exception:
        return date_str


def main():
    """메인 함수"""
    # 세션 상태 초기화
    if "user_info" not in st.session_state:
        st.session_state.user_info = None
    if "search_triggered" not in st.session_state:
        st.session_state.search_triggered = False
    if "toast_message" not in st.session_state:
        st.session_state.toast_message = None

    # 토스트 메시지가 있으면 표시하고 제거
    if st.session_state.toast_message:
        st.toast(st.session_state.toast_message, icon="✅")
        st.session_state.toast_message = None

    # 로그인 상태 확인
    if st.session_state.user_info is None:
        render_login_page()
    else:
        render_dashboard_page()


if __name__ == "__main__":
    main()
