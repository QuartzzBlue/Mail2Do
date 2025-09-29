# functions/api/function_app.py
import azure.functions as func
import logging
import json
import os
from typing import Dict, List, Optional
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from azure.core.credentials import AzureKeyCredential
from azure.data.tables import TableServiceClient
from openai import AzureOpenAI

app = func.FunctionApp()

# 환경 변수 로드
AI_SEARCH_ENDPOINT = os.getenv("AI_SEARCH_ENDPOINT")
AI_SEARCH_INDEX = os.getenv("AI_SEARCH_INDEX", "emails-index")
AI_SEARCH_ADMIN_KEY = os.getenv("AI_SEARCH_ADMIN_KEY")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT_EMB = os.getenv(
    "AZURE_OPENAI_DEPLOYMENT_EMB", "text-embedding-3-small"
)

# 클라이언트 초기화
search_client = SearchClient(
    endpoint=AI_SEARCH_ENDPOINT,
    index_name=AI_SEARCH_INDEX,
    credential=AzureKeyCredential(AI_SEARCH_ADMIN_KEY),
)

table_service = TableServiceClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING
)

openai_client = AzureOpenAI(
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
    api_version="2024-02-01",
)


@app.route(route="email/{emailId}", methods=["GET"])
def get_email_detail(req: func.HttpRequest) -> func.HttpResponse:
    """이메일 상세 조회 API"""

    try:
        email_id = req.route_params.get("emailId")

        # 해당 이메일의 모든 청크 조회
        results = search_client.search(
            search_text="*",
            filter=f"emailId eq '{email_id}'",
            select=[
                "emailId",
                "subject",
                "from_name",
                "from_email",
                "to_names",
                "cc_names",
                "receivedAt",
                "html_body",
                "webLink",
                "chunk",
            ],
            top=50,
        )

        chunks = []
        email_info = None

        for result in results:
            if not email_info:
                email_info = {
                    "emailId": result["emailId"],
                    "subject": result["subject"],
                    "from_name": result["from_name"],
                    "from_email": result["from_email"],
                    "to_names": result["to_names"],
                    "cc_names": result["cc_names"],
                    "receivedAt": result["receivedAt"],
                    "html_body": result.get("html_body", ""),
                    "webLink": result.get("webLink", ""),
                }

            chunks.append(result["chunk"])

        if not email_info:
            return func.HttpResponse(
                json.dumps({"error": "이메일을 찾을 수 없습니다"}),
                mimetype="application/json",
                status_code=404,
            )

        # 전체 본문 재구성
        email_info["full_body"] = "\\n\\n".join(chunks)

        return func.HttpResponse(
            json.dumps(email_info), mimetype="application/json", status_code=200
        )

    except Exception as e:
        logging.error(f"이메일 상세 조회 실패: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), mimetype="application/json", status_code=500
        )


@app.route(route="login", methods=["POST"])
def user_login(req: func.HttpRequest) -> func.HttpResponse:
    """사용자 로그인 API - Employees 테이블 조회"""

    try:
        req_body = req.get_json()
        email = req_body.get("email", "").strip().lower()

        if not email:
            return func.HttpResponse(
                json.dumps({"error": "이메일을 입력해주세요"}),
                mimetype="application/json",
                status_code=400,
            )

        # Employees 테이블에서 사용자 조회
        employees_table = table_service.get_table_client("Employees")

        try:
            # 이메일을 RowKey로 사용하여 직접 조회
            entity = employees_table.get_entity(partition_key="techcorp", row_key=email)

            user_info = {
                "name": entity.get("name", ""),
                "email": entity.get("email", ""),
                "team_name": entity.get("team_name", ""),
                "original_partition_key": entity.get("original_partition_key", ""),
            }

            logging.info(
                f"사용자 로그인 성공: {user_info['name']} ({user_info['email']})"
            )

            return func.HttpResponse(
                json.dumps(user_info), mimetype="application/json", status_code=200
            )

        except Exception as table_error:
            # 직접 조회 실패 시 전체 테이블에서 검색
            logging.warning(f"직접 조회 실패, 전체 검색 시도: {table_error}")

            # 모든 직원 중에서 이메일 매칭 검색
            for entity in employees_table.list_entities():
                if entity.get("email", "").lower() == email:
                    user_info = {
                        "name": entity.get("name", ""),
                        "email": entity.get("email", ""),
                        "team_name": entity.get("team_name", ""),
                        "original_partition_key": entity.get(
                            "original_partition_key", ""
                        ),
                    }

                    logging.info(
                        f"사용자 로그인 성공 (검색): {user_info['name']} ({user_info['email']})"
                    )

                    return func.HttpResponse(
                        json.dumps(user_info),
                        mimetype="application/json",
                        status_code=200,
                    )

            # 사용자를 찾지 못한 경우
            logging.warning(f"등록되지 않은 사용자: {email}")
            return func.HttpResponse(
                json.dumps(
                    {
                        "error": "등록되지 않은 사용자입니다",
                        "suggestion": "관리자에게 계정 등록을 요청하세요",
                    }
                ),
                mimetype="application/json",
                status_code=404,
            )

    except Exception as e:
        logging.error(f"로그인 처리 실패: {e}")
        return func.HttpResponse(
            json.dumps({"error": f"로그인 처리 중 오류가 발생했습니다: {str(e)}"}),
            mimetype="application/json",
            status_code=500,
        )


@app.route(route="users", methods=["GET"])
def list_users(req: func.HttpRequest) -> func.HttpResponse:
    """사용자 목록 조회 API (개발/테스트용)"""

    try:
        # 쿼리 파라미터
        team_filter = req.params.get("team", "")
        limit = int(req.params.get("limit", "20"))

        employees_table = table_service.get_table_client("Employees")

        users = []
        count = 0

        for entity in employees_table.list_entities():
            if count >= limit:
                break

            # 팀 필터 적용
            if team_filter and team_filter not in entity.get("team_name", ""):
                continue

            user_info = {
                "name": entity.get("name", ""),
                "email": entity.get("email", ""),
                "team_name": entity.get("team_name", ""),
            }

            users.append(user_info)
            count += 1

        return func.HttpResponse(
            json.dumps(
                {"users": users, "total_count": count, "team_filter": team_filter}
            ),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"사용자 목록 조회 실패: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), mimetype="application/json", status_code=500
        )


@app.route(route="teams", methods=["GET"])
def list_teams(req: func.HttpRequest) -> func.HttpResponse:
    """팀 목록 조회 API"""

    try:
        teams_table = table_service.get_table_client("Teams")

        teams = []

        for entity in teams_table.list_entities():
            team_info = {
                "team_name": entity.get("team_name", ""),
                "partition_key": entity.get("PartitionKey", ""),
                "row_key": entity.get("RowKey", ""),
            }
            teams.append(team_info)

        # 팀명으로 정렬
        teams.sort(key=lambda x: x["team_name"])

        return func.HttpResponse(
            json.dumps({"teams": teams, "total_count": len(teams)}),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"팀 목록 조회 실패: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}), mimetype="application/json", status_code=500
        )


@app.route(route="search", methods=["POST"])
def search_emails(req: func.HttpRequest) -> func.HttpResponse:
    """이메일 검색 API"""

    try:
        req_body = req.get_json()
        query = req_body.get("query", "")
        filters = req_body.get("filters", {})
        user_email = req_body.get("user_email", "")

        logging.info(f"검색 요청: {query}, 사용자: {user_email}, 필터: {filters}")

        # --- OData 문자열 이스케이프(따옴표 -> 두 개의 따옴표) ---
        def escape_odata_str(s: str) -> str:
            return (s or "").replace("'", "''")

        # OData 필터 구성
        filter_conditions = []

        # 사용자별 필터 - assignee 필드에 이메일이 포함되어 있는지 검색
        if filters.get("assign_filter") == "me":
            safe_email = escape_odata_str(user_email)
            # search.in 사용하여 부분 일치 검색
            filter_conditions.append(
                f"(search.ismatch('{safe_email}', 'assignee') or assignee eq '미지정')"
            )
        elif filters.get("assign_filter") == "unassigned":
            filter_conditions.append("assignee eq '미지정'")
        # "all"인 경우 필터 추가하지 않음

        # 액션 타입 필터
        if filters.get("action_types"):
            types = filters["action_types"]
            if types:
                type_conditions = " or ".join(
                    [f"action_type eq '{escape_odata_str(t)}'" for t in types if t]
                )
                if type_conditions:
                    filter_conditions.append(f"({type_conditions})")

        # 우선순위 필터
        if filters.get("priorities"):
            priorities = filters["priorities"]
            if priorities:
                priority_conditions = " or ".join(
                    [f"priority eq '{escape_odata_str(p)}'" for p in priorities if p]
                )
                if priority_conditions:
                    filter_conditions.append(f"({priority_conditions})")

        # 완료 상태 필터 (done 필드가 있다고 가정)
        if filters.get("completion_filter") == "complete":
            filter_conditions.append("done eq true")
        elif filters.get("completion_filter") == "incomplete":
            filter_conditions.append("done eq false")
        # "all"인 경우 필터 추가하지 않음

        # 날짜 범위 필터
        if filters.get("no_due"):
            # 기한 미지정 (due가 null인 경우는 OData에서 직접 필터링 불가, 결과 후처리 필요)
            pass
        else:
            if filters.get("date_from"):
                filter_conditions.append(f"due ge {filters['date_from']}T00:00:00Z")
            if filters.get("date_to"):
                filter_conditions.append(f"due le {filters['date_to']}T23:59:59Z")

        odata_filter = " and ".join(filter_conditions) if filter_conditions else None

        # 디버깅용 로그
        logging.info(f"생성된 OData 필터: {odata_filter}")

        # 검색 실행
        if query.strip():
            # 임베딩 생성 (실패 시 시맨틱-only 폴백)
            vector_queries = None
            try:
                embedding_response = openai_client.embeddings.create(
                    model=AZURE_OPENAI_DEPLOYMENT_EMB, input=[query]
                )
                query_embedding = embedding_response.data[0].embedding

                vector_queries = [
                    VectorizedQuery(
                        vector=query_embedding,
                        k_nearest_neighbors=10,
                        fields="chunkEmbedding",
                    )
                ]
            except Exception as emb_ex:
                logging.warning(f"임베딩 생성 실패, 시맨틱 검색으로 폴백: {emb_ex}")

            if vector_queries:
                results = search_client.search(
                    search_text=query,
                    vector_queries=vector_queries,
                    query_type="semantic",
                    semantic_configuration_name="semantic-config",
                    query_caption="extractive",
                    query_answer="extractive",
                    filter=odata_filter,
                    top=20,
                )
            else:
                results = search_client.search(
                    search_text=query,
                    query_type="semantic",
                    semantic_configuration_name="semantic-config",
                    query_caption="extractive",
                    query_answer="extractive",
                    filter=odata_filter,
                    top=20,
                )
        else:
            # 필터링만 수행
            results = search_client.search(
                search_text="*",
                filter=odata_filter,
                order_by=["receivedAt desc"],
                top=50,
            )

        # 결과 포맷팅
        formatted_results = []
        for result in results:
            # no_due 필터 처리 (결과 후처리)
            if filters.get("no_due"):
                if result.get("due"):  # due가 있으면 스킵
                    continue

            # assignee에서 이메일만 추출
            assignee_raw = result.get("assignee", "미지정")
            assignee_display = assignee_raw
            assignee_email = ""

            # "이름 <email>" 형식에서 이메일 추출
            if "<" in assignee_raw and ">" in assignee_raw:
                import re

                match = re.search(r"<([^>]+)>", assignee_raw)
                if match:
                    assignee_email = match.group(1)
                assignee_display = assignee_raw.split("<")[0].strip()

            item = {
                "id": result.get("id"),
                "emailId": result.get("emailId"),
                "subject": result.get("subject"),
                "from_name": result.get("from_name"),
                "to_names": result.get("to_names", []),
                "receivedAt": result.get("receivedAt"),
                "bodyPreview": result.get("bodyPreview"),
                "action": result.get("action", ""),
                "action_type": result.get("action_type", ""),
                "assignee": assignee_display,
                "assignee_email": assignee_email,
                "due": result.get("due"),
                "priority": result.get("priority", ""),
                "tags": result.get("tags", []),
                "confidence": result.get("confidence", 0.0),
                "done": result.get("done", False),
                "score": result.get("@search.score", 0.0),
            }

            # 시맨틱 캡션 추가
            caps = result.get("@search.captions")
            if caps:
                captions = []
                for caption in caps:
                    text = getattr(caption, "text", None) or caption.get("text")
                    highlights = getattr(caption, "highlights", None) or caption.get(
                        "highlights"
                    )
                    captions.append({"text": text, "highlights": highlights})
                item["captions"] = captions

            formatted_results.append(item)

        logging.info(f"검색 결과 수: {len(formatted_results)}")

        return func.HttpResponse(
            json.dumps(
                {"results": formatted_results, "total_count": len(formatted_results)},
                ensure_ascii=False,
            ),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"검색 실패: {e}")
        import traceback

        logging.error(traceback.format_exc())
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False),
            mimetype="application/json",
            status_code=500,
        )


@app.route(route="dashboard", methods=["POST"])
def get_dashboard_data(req: func.HttpRequest) -> func.HttpResponse:
    """대시보드 데이터 API"""

    try:
        req_body = req.get_json()
        user_email = req_body.get("user_email", "")

        # 기본 필터링 - assignee에 이메일이 포함되어 있는지 검색
        def escape_odata_str(s: str) -> str:
            return (s or "").replace("'", "''")

        safe_email = escape_odata_str(user_email)

        # search.ismatch를 사용하여 부분 일치 검색
        base_filter = f"(search.ismatch('{safe_email}', 'assignee') or assignee eq '미지정') and action_type eq 'DO' and (priority eq 'High' or priority eq 'Medium')"

        # 이번 주 필터 추가
        from datetime import datetime, timedelta

        today = datetime.now().date()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)

        date_filter = (
            f" and due ge {week_start}T00:00:00Z and due le {week_end}T23:59:59Z"
        )
        final_filter = base_filter + date_filter

        logging.info(f"대시보드 필터: {final_filter}")

        # 검색 실행
        results = search_client.search(
            search_text="*",
            filter=final_filter,
            order_by=["due asc"],
            select=[
                "id",
                "emailId",
                "subject",
                "from_name",
                "receivedAt",
                "action_type",
                "action",
                "assignee",
                "due",
                "priority",
                "tags",
                "confidence",
                "bodyPreview",
                "done",
            ],
            top=20,
        )

        # 결과 포맷팅
        dashboard_items = []
        for result in results:
            # assignee에서 이메일만 추출
            assignee_raw = result.get("assignee", "미지정")
            assignee_display = assignee_raw

            if "<" in assignee_raw and ">" in assignee_raw:
                assignee_display = assignee_raw.split("<")[0].strip()

            dashboard_items.append(
                {
                    "id": result["id"],
                    "emailId": result["emailId"],
                    "subject": result["subject"],
                    "from_name": result["from_name"],
                    "receivedAt": result["receivedAt"],
                    "bodyPreview": result["bodyPreview"],
                    "action": result.get("action", ""),
                    "action_type": result["action_type"],
                    "assignee": assignee_display,
                    "due": result.get("due"),
                    "priority": result["priority"],
                    "tags": result.get("tags", []),
                    "confidence": result.get("confidence", 0.0),
                    "done": result.get("done", False),
                }
            )

        logging.info(f"대시보드 결과 수: {len(dashboard_items)}")

        return func.HttpResponse(
            json.dumps(
                {
                    "items": dashboard_items,
                    "filter_applied": final_filter,
                    "count": len(dashboard_items),
                },
                ensure_ascii=False,
            ),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"대시보드 데이터 조회 실패: {e}")
        import traceback

        logging.error(traceback.format_exc())
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False),
            mimetype="application/json",
            status_code=500,
        )
