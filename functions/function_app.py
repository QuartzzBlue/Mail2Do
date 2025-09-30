# functions/api/function_app.py
import azure.functions as func
import logging
import json
import os
import re
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from azure.core.credentials import AzureKeyCredential
from azure.data.tables import TableServiceClient
from openai import AzureOpenAI

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

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


def escape_odata_string(value: str) -> str:
    """OData 쿼리용 문자열 이스케이프"""
    if not value:
        return ""
    return value.replace("'", "''")


def get_action_done_status(action_id: str) -> bool:
    """Table Storage에서 액션의 완료 상태 조회"""
    try:
        actions_table = table_service.get_table_client("Actions")
        entity = actions_table.get_entity(partition_key="techcorp", row_key=action_id)
        return entity.get("done", False)
    except Exception as e:
        logging.debug(f"액션 {action_id}의 done 상태 조회 실패: {e}")
        return False


def format_search_result(result: dict, include_captions: bool = False) -> dict:
    """검색 결과를 프론트엔드 형식으로 가공하여 리턴"""

    assignee_raw = result.get("assignee")  # 박지훈 <jihoon.park@techcorp.com>

    # None 처리를 먼저
    if not assignee_raw or assignee_raw == "미지정":
        assignee_display = "미지정"
        assignee_email = ""
    elif "<" in assignee_raw and ">" in assignee_raw:

        match = re.search(r"<([^>]+)>", assignee_raw)
        if match:
            assignee_email = match.group(1).strip()
        else:
            assignee_email = ""
        assignee_display = assignee_raw.split("<")[0].strip()
    else:
        # 이메일만 있는 경우
        assignee_display = assignee_raw
        assignee_email = assignee_raw

    # done 상태 조회
    action_id = result.get("id", "")
    done_status = get_action_done_status(action_id)

    item = {
        "id": action_id,
        "emailId": result.get("emailId", ""),
        "subject": result.get("subject", "제목 없음"),
        "from_name": result.get("from_name", ""),
        "to_names": result.get("to_names", []),
        "receivedAt": result.get("receivedAt", ""),
        "bodyPreview": result.get("bodyPreview", ""),
        "action": result.get("action", ""),
        "actionType": result.get("action_type", "DO"),
        "assignee": assignee_display,  # 항상 문자열
        "assignee_email": assignee_email,  # 항상 문자열 (빈 문자열 또는 이메일)
        "assignee_raw": assignee_raw if assignee_raw else "",  # 항상 문자열
        "due": result.get("due"),
        "priority": result.get("priority", "Medium"),
        "tags": result.get("tags", []),
        "confidence": result.get("confidence", 0.0),
        "done": done_status,
        "score": result.get("@search.score", 0.0),
    }

    # 시맨틱 캡션 추가
    if include_captions:
        caps = result.get("@search.captions")
        if caps:
            captions = []
            for caption in caps:
                text = getattr(caption, "text", None) or caption.get("text", "")
                highlights = getattr(caption, "highlights", None) or caption.get(
                    "highlights", ""
                )
                captions.append({"text": text, "highlights": highlights})
            item["captions"] = captions

    return item


@app.route(route="login", methods=["POST"])
def user_login(req: func.HttpRequest) -> func.HttpResponse:
    """사용자 로그인 API - Employees 테이블 조회하여 일치하는 메일이 있다면 로그인 처리 진행"""

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


@app.route(route="dashboard", methods=["POST"])
def get_dashboard_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    대시보드() 데이터 조회 API
    """
    try:
        req_body = req.get_json()
        user_email = req_body.get("user_email", "")

        if not user_email:
            return func.HttpResponse(
                json.dumps({"error": "user_email이 필요합니다"}, ensure_ascii=False),
                mimetype="application/json",
                status_code=400,
            )

        results = search_client.search(search_text="*", filter=None, top=100)

        dashboard_items = []

        for result in results:
            item = format_search_result(result, include_captions=False)
            dashboard_items.append(item)

        logging.info(f"대시보드 결과 수: {len(dashboard_items)})")

        return func.HttpResponse(
            json.dumps(
                {"items": dashboard_items, "count": len(dashboard_items)},
                ensure_ascii=False,
            ),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"대시보드 조회 실패: {e}")
        import traceback

        logging.error(traceback.format_exc())
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False),
            mimetype="application/json",
            status_code=500,
        )


@app.route(route="search", methods=["POST"])
def search_emails(req: func.HttpRequest) -> func.HttpResponse:
    """
    이메일 검색 API
    """
    try:
        req_body = req.get_json()
        query = req_body.get("query", "").strip()
        filters = req_body.get("filters", {})
        user_email = req_body.get("user_email", "")

        if not user_email:
            return func.HttpResponse(
                json.dumps({"error": "user_email이 필요합니다"}, ensure_ascii=False),
                mimetype="application/json",
                status_code=400,
            )

        logging.info(f"검색 요청 - 사용자: {user_email}, 쿼리: '{query}'")

        # 검색 실행
        if query:
            # 쿼리가 있는 경우: 벡터 + 시맨틱 검색
            vector_queries = None

            try:
                # 임베딩 생성
                embedding_response = openai_client.embeddings.create(
                    model=AZURE_OPENAI_DEPLOYMENT_EMB, input=[query]
                )
                query_embedding = embedding_response.data[0].embedding

                vector_queries = [
                    VectorizedQuery(
                        vector=query_embedding,
                        k_nearest_neighbors=20,
                        fields="chunkEmbedding",
                    )
                ]
                logging.info("벡터 검색 활성화")
            except Exception as emb_ex:
                logging.warning(f"임베딩 생성 실패, 텍스트 검색으로 폴백: {emb_ex}")

            # 검색 실행
            if vector_queries:
                results = search_client.search(
                    search_text=query,
                    vector_queries=vector_queries,
                    query_type="semantic",
                    semantic_configuration_name="semantic-config",
                    query_caption="extractive",
                    query_answer="extractive",
                    top=50,
                )
            else:
                results = search_client.search(
                    search_text=query,
                    query_type="semantic",
                    semantic_configuration_name="semantic-config",
                    query_caption="extractive",
                    query_answer="extractive",
                    top=50,
                )
        else:
            results = search_client.search(
                search_text="*",
                order_by=["receivedAt desc"],
                top=100,
            )

        # 클라이언트 측 필터링
        formatted_results = []

        for result in results:
            item = format_search_result(result, include_captions=False)
            formatted_results.append(item)

        logging.info(f"검색 결과 수: {len(formatted_results)} (필터링 후)")

        return func.HttpResponse(
            json.dumps(
                {
                    "results": formatted_results,
                    "total_count": len(formatted_results),
                    "query": query,
                    "filters_applied": filters,
                },
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


@app.route(route="action/{actionId}", methods=["PATCH"])
def update_action_status(req: func.HttpRequest) -> func.HttpResponse:
    """
    액션 완료 상태 업데이트
    체크박스 클릭 시 호출
    """
    try:
        action_id = req.route_params.get("actionId")
        req_body = req.get_json()
        done = req_body.get("done", False)

        if not action_id:
            return func.HttpResponse(
                json.dumps({"error": "actionId가 필요합니다"}, ensure_ascii=False),
                mimetype="application/json",
                status_code=400,
            )

        logging.info(f"액션 상태 업데이트 요청 - ID: {action_id}, done: {done}")

        # Table Storage 업데이트
        actions_table = table_service.get_table_client("Actions")

        entity = {
            "PartitionKey": "techcorp",
            "RowKey": action_id,
            "done": done,
            "updatedAt": datetime.utcnow().isoformat(),
        }

        # upsert: 없으면 생성, 있으면 업데이트
        actions_table.upsert_entity(entity, mode="merge")

        logging.info(f"액션 {action_id} 상태 업데이트 완료: done={done}")

        return func.HttpResponse(
            json.dumps(
                {"success": True, "action_id": action_id, "done": done},
                ensure_ascii=False,
            ),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"액션 상태 업데이트 실패: {e}")
        import traceback

        logging.error(traceback.format_exc())
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False),
            mimetype="application/json",
            status_code=500,
        )


@app.route(route="email/{emailId}", methods=["GET"])
def get_email_detail(req: func.HttpRequest) -> func.HttpResponse:
    """이메일 상세 정보 조회"""
    try:
        email_id = req.route_params.get("emailId")

        if not email_id:
            return func.HttpResponse(
                json.dumps({"error": "emailId가 필요합니다"}, ensure_ascii=False),
                mimetype="application/json",
                status_code=400,
            )

        logging.info(f"이메일 상세 조회 - ID: {email_id}")

        # emailId로 검색
        results = search_client.search(
            search_text="*",
            filter=f"emailId eq '{escape_odata_string(email_id)}'",
            top=1,
        )

        result = None
        for r in results:
            result = r
            break

        if not result:
            return func.HttpResponse(
                json.dumps({"error": "이메일을 찾을 수 없습니다"}, ensure_ascii=False),
                mimetype="application/json",
                status_code=404,
            )

        detail = {
            "emailId": result.get("emailId"),
            "subject": result.get("subject"),
            "from_name": result.get("from_name"),
            "from_email": result.get("from_email"),
            "to_names": result.get("to_names", []),
            "to_emails": result.get("to_emails", []),
            "cc_names": result.get("cc_names", []),
            "cc_emails": result.get("cc_emails", []),
            "receivedAt": result.get("receivedAt"),
            "full_body": result.get("body", ""),
            "html_body": result.get("html_body", ""),
            "bodyPreview": result.get("bodyPreview", ""),
        }

        return func.HttpResponse(
            json.dumps(detail, ensure_ascii=False),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"이메일 상세 조회 실패: {e}")
        import traceback

        logging.error(traceback.format_exc())
        return func.HttpResponse(
            json.dumps({"error": str(e)}, ensure_ascii=False),
            mimetype="application/json",
            status_code=500,
        )
