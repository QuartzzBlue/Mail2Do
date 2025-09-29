# scripts/create_search_index.py
import os
from dotenv import load_dotenv
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    SimpleField,
    SearchableField,
    ComplexField,
    VectorSearch,
    VectorSearchProfile,
    HnswAlgorithmConfiguration,
    SemanticConfiguration,
    SemanticSearch,
    SemanticField,
    SemanticPrioritizedFields
)
from azure.core.credentials import AzureKeyCredential

load_dotenv()

def create_email_search_index():

    search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
    search_key = os.getenv("AI_SEARCH_ADMIN_KEY")
    index_name = "emails-index"

    credential = AzureKeyCredential(search_key)
    client = SearchIndexClient(endpoint=search_endpoint, credential=credential)

    # 벡터 검색 설정
    vector_search = VectorSearch(
        profiles=[
            VectorSearchProfile(
                name="embedding-profile",
                algorithm_configuration_name="embedding-hnsw"
            )
        ],
        algorithms=[
            HnswAlgorithmConfiguration(name="embedding-hnsw")
        ]
    )

    # 시맨틱 검색 설정
    semantic_search = SemanticSearch(
        configurations=[
            SemanticConfiguration(
                name="semantic-config",
                prioritized_fields=SemanticPrioritizedFields(
                    title_field=SemanticField(field_name="subject"),
                    content_fields=[
                        SemanticField(field_name="chunk"),
                        SemanticField(field_name="bodyPreview")
                    ]
                )
            )
        ]
    )

    # 인덱스 필드 정의
    fields = [
        # 키 필드
        SimpleField(
            name="id",
            type=SearchFieldDataType.String,
            key=True
        ),
        SimpleField(
            name="emailId",
            type=SearchFieldDataType.String,
            filterable=True,
            facetable=True
        ),
        SimpleField(
            name="conversationId",
            type=SearchFieldDataType.String,
            filterable=True,
            facetable=True
        ),

        # 검색 가능한 텍스트 필드
        SearchableField(
            name="subject",
            type=SearchFieldDataType.String,
            searchable=True,
            analyzer_name="ko.microsoft"  # 한국어 분석기
        ),
        SearchableField(
            name="chunk",
            type=SearchFieldDataType.String,
            searchable=True,
            analyzer_name="ko.microsoft"
        ),
        SearchableField(
            name="bodyPreview",
            type=SearchFieldDataType.String,
            searchable=True,
            analyzer_name="ko.microsoft"
        ),

        # 벡터 임베딩 필드
        SearchField(
            name="chunkEmbedding",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=1536,
            vector_search_profile_name="embedding-profile"
        ),

        # 필터링 필드
        SimpleField(
            name="action_type",
            type=SearchFieldDataType.String,
            filterable=True,
            facetable=True
        ),
        SearchableField(
            name="action",
            type=SearchFieldDataType.String,
            filterable=True,
            facetable=True,
            searchable=True
        ),
        SearchableField(
            name="assignee",
            type=SearchFieldDataType.String,
            filterable=True,
            facetable=True,
            searchable=True
        ),
        SimpleField(
            name="due",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True
        ),
        SimpleField(
            name="priority",
            type=SearchFieldDataType.String,
            filterable=True,
            facetable=True
        ),
        SearchField(
            name="tags",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            filterable=True,
            facetable=True,
            searchable=True
        ),
        SimpleField(
            name="confidence",
            type=SearchFieldDataType.Double,
            filterable=True,
            sortable=True
        ),

        # 메타데이터 필드
        SimpleField(
            name="from_name",
            type=SearchFieldDataType.String,
            filterable=True,
            facetable=True
        ),
        SimpleField(
            name="from_email",
            type=SearchFieldDataType.String,
            filterable=True
        ),
        SearchField(
            name="to_names",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            filterable=True,
            searchable=True
        ),
        SearchField(
            name="cc_names",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            filterable=True,
            searchable=True
        ),
        SimpleField(
            name="receivedAt",
            type=SearchFieldDataType.DateTimeOffset,
            filterable=True,
            sortable=True
        ),
        SimpleField(
            name="webLink",
            type=SearchFieldDataType.String
        ),
        SimpleField(
            name="html_body",
            type=SearchFieldDataType.String
        )
    ]

    # 인덱스 생성
    index = SearchIndex(
        name=index_name,
        fields=fields,
        vector_search=vector_search,
        semantic_search=semantic_search
    )

    try:
        result = client.create_index(index)
        print(f"인덱스 '{index_name}' 생성 완료")
        return result
    except Exception as e:
        print(f"인덱스 생성 실패: {e}")
        raise

if __name__ == "__main__":
    create_email_search_index()