import os
import csv
from dotenv import load_dotenv
from azure.data.tables import TableServiceClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

def setup_table_storage():
    """Azure Table Storage 테이블 생성 및 CSV 데이터 로드"""
    
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    table_service = TableServiceClient.from_connection_string(connection_string)
    
    # 테이블 목록
    tables = ["Employees", "Teams", "Actions"]
    
    print("=== Azure Table Storage 설정 시작 ===")
    
    # 테이블 생성
    for table_name in tables:
        try:
            table_service.create_table(table_name)
        except Exception as e:
            if "EntityAlreadyExists" in str(e) or "TableAlreadyExists" in str(e):
                print(f"테이블 '{table_name}' 이미 존재")
            else:
                print(f"테이블 '{table_name}' 생성 실패: {e}")
    
    # Teams 데이터 로드
    load_teams_data(table_service)
    
    # Employees 데이터 로드  
    print("\n=== Employees 데이터 로드 ===")
    load_employees_data(table_service)
    
    print("\n설정 완료")

def load_teams_data(table_service): 
    teams_table = table_service.get_table_client("Teams")
    
    try:
        with open("../data/Teams.csv", "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            
            inserted_count = 0
            error_count = 0
            
            for row in reader:
                try:
                    entity = {
                        "PartitionKey": str(row["PartitionKey"]).strip(),  # "ORG"
                        "RowKey": str(row["RowKey"]).strip(),              # "1", "2", etc.
                        "team_name": str(row["team_name"]).strip()
                    }
                    
                    # 엔티티 삽입
                    teams_table.upsert_entity(entity)
                    inserted_count += 1

                except Exception as e:
                    error_count += 1
                    print(f"   --- 팀 데이터 삽입 실패: {row} - {e}")
            
            print(f"Teams 데이터 로드 완료: {inserted_count}개 성공, {error_count}개 실패")
            
    except FileNotFoundError:
        print("   --- Teams 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"   --- Teams 데이터 로드 실패: {e}")

def load_employees_data(table_service):
    
    employees_table = table_service.get_table_client("Employees")
    
    try:
        with open("../data/Employees.csv", "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            
            inserted_count = 0
            error_count = 0
            
            for row in reader:
                try:
                    # CSV 데이터를 Table Storage 엔티티로 변환
                    entity = {
                        "PartitionKey": "techcorp",  # 회사별로 파티셔닝
                        "RowKey": str(row["email"]).strip(),  # 이메일을 RowKey로 사용
                        "name": str(row["name"]).strip(),
                        "email": str(row["email"]).strip(),
                        "team_name": str(row["team_name"]).strip(),
                        # 원본 PartitionKey를 별도 필드로 보존
                        "original_partition_key": str(row["PartitionKey"]).strip()
                    }
                    
                    # 엔티티 삽입
                    employees_table.upsert_entity(entity)
                    inserted_count += 1
                    
                except Exception as e:
                    error_count += 1
                    print(f"   --- 직원 데이터 삽입 실패: {row} - {e}")
            
            print(f"Employees 데이터 로드 완료: {inserted_count}개 성공, {error_count}개 실패")
            
    except FileNotFoundError:
        print("   --- Employees.csv 파일을 찾을 수 없습니다.")
    except Exception as e:
        print(f"   ---  Employees 데이터 로드 실패: {e}")

if __name__ == "__main__":

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("AZURE_STORAGE_CONNECTION_STRING 환경 변수 에러")
        exit(1)
    
    table_service = TableServiceClient.from_connection_string(connection_string)

    setup_table_storage()