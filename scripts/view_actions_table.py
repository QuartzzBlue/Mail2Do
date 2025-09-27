# scripts/view_actions.py
"""
Azure Table Storage의 Actions 테이블 조회 및 표시
"""

import os
import json
from dotenv import load_dotenv
from datetime import datetime
from azure.data.tables import TableServiceClient
import pandas as pd

load_dotenv()

class ActionsViewer:
    """Actions 테이블 조회 클래스"""
    
    def __init__(self):
        """초기화"""
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        
        if not self.connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING 환경 변수가 설정되지 않았습니다.")
        
        self.table_service = TableServiceClient.from_connection_string(self.connection_string)
        self.actions_table = self.table_service.get_table_client("Actions")
    
    def get_all_actions(self):
        """모든 액션 조회"""
        
        print("📊 Actions 테이블 전체 조회 중...")
        
        try:
            entities = list(self.actions_table.list_entities())
            print(f"✅ 총 {len(entities)}개 액션 발견")
            return entities
            
        except Exception as e:
            print(f"❌ Actions 테이블 조회 실패: {e}")
            return []
    
    def display_actions_table(self, entities):
        """액션들을 테이블 형태로 표시"""
        
        if not entities:
            print("📭 액션이 없습니다.")
            return
        
        print("\n" + "="*120)
        print("📋 Actions 테이블 내용")
        print("="*120)
        
        # 헤더
        headers = ["No", "제목", "담당자", "마감일", "우선순위", "타입", "신뢰도", "태그"]
        
        # 헤더 출력
        print(f"{headers[0]:<4} {headers[1]:<40} {headers[2]:<25} {headers[3]:<12} {headers[4]:<8} {headers[5]:<10} {headers[6]:<8} {headers[7]:<20}")
        print("-" * 120)
        
        # 데이터 출력
        for i, entity in enumerate(entities, 1):
            title = entity.get('title', '')[:38] + ('...' if len(entity.get('title', '')) > 38 else '')
            assignee = entity.get('assignee', '')[:23] + ('...' if len(entity.get('assignee', '')) > 23 else '')
            due = entity.get('due', '')[:10] if entity.get('due') else ''
            priority = entity.get('priority', '')[:6]
            action_type = entity.get('type', '')[:8]
            confidence = f"{entity.get('confidence', 0):.2f}"
            tags = entity.get('tags', '')[:18] + ('...' if len(entity.get('tags', '')) > 18 else '')
            
            print(f"{i:<4} {title:<40} {assignee:<25} {due:<12} {priority:<8} {action_type:<10} {confidence:<8} {tags:<20}")
    
    def display_actions_detailed(self, entities):
        """액션들을 상세하게 표시"""
        
        if not entities:
            print("📭 액션이 없습니다.")
            return
        
        print("\n" + "="*80)
        print("📋 Actions 상세 내용")
        print("="*80)
        
        for i, entity in enumerate(entities, 1):
            print(f"\n🔸 액션 #{i}")
            print(f"   제목: {entity.get('title', 'N/A')}")
            print(f"   이메일 제목: {entity.get('subject', 'N/A')}")
            print(f"   담당자: {entity.get('assignee', 'N/A')}")
            print(f"   마감일: {entity.get('due', 'N/A')}")
            print(f"   우선순위: {entity.get('priority', 'N/A')}")
            print(f"   타입: {entity.get('type', 'N/A')}")
            print(f"   신뢰도: {entity.get('confidence', 'N/A')}")
            print(f"   태그: {entity.get('tags', 'N/A')}")
            print(f"   수신일: {entity.get('receivedAt', 'N/A')}")
            print(f"   파티션 키: {entity.get('PartitionKey', 'N/A')}")
            print(f"   행 키: {entity.get('RowKey', 'N/A')}")
            print("-" * 80)
    
    def get_actions_by_assignee(self, assignee_filter):
        """담당자별 액션 조회"""
        
        print(f"🔍 담당자 '{assignee_filter}'의 액션 조회 중...")
        
        try:
            # 필터 쿼리 사용
            filter_query = f"assignee eq '{assignee_filter}'"
            entities = list(self.actions_table.query_entities(filter_query))
            
            print(f"✅ {len(entities)}개 액션 발견")
            return entities
            
        except Exception as e:
            print(f"❌ 필터 조회 실패: {e}")
            return []
    
    def get_actions_by_priority(self, priority_filter):
        """우선순위별 액션 조회"""
        
        print(f"🔍 우선순위 '{priority_filter}'의 액션 조회 중...")
        
        try:
            filter_query = f"priority eq '{priority_filter}'"
            entities = list(self.actions_table.query_entities(filter_query))
            
            print(f"✅ {len(entities)}개 액션 발견")
            return entities
            
        except Exception as e:
            print(f"❌ 필터 조회 실패: {e}")
            return []
    
    def export_to_csv(self, entities, filename="actions_export.csv"):
        """CSV로 내보내기"""
        
        if not entities:
            print("내보낼 데이터가 없습니다.")
            return
        
        try:
            # pandas DataFrame으로 변환
            data = []
            for entity in entities:
                data.append({
                    'title': entity.get('title', ''),
                    'subject': entity.get('subject', ''),
                    'assignee': entity.get('assignee', ''),
                    'due': entity.get('due', ''),
                    'priority': entity.get('priority', ''),
                    'type': entity.get('type', ''),
                    'confidence': entity.get('confidence', ''),
                    'tags': entity.get('tags', ''),
                    'receivedAt': entity.get('receivedAt', ''),
                    'partitionKey': entity.get('PartitionKey', ''),
                    'rowKey': entity.get('RowKey', '')
                })
            
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            
            print(f"✅ CSV 파일로 내보내기 완료: {filename}")
            
        except Exception as e:
            print(f"❌ CSV 내보내기 실패: {e}")
    
    def export_to_json(self, entities, filename="actions_export.json"):
        """JSON으로 내보내기"""
        
        if not entities:
            print("내보낼 데이터가 없습니다.")
            return
        
        try:
            # 엔티티를 일반 dict로 변환
            data = []
            for entity in entities:
                clean_entity = {}
                for key, value in entity.items():
                    if not key.startswith('odata') and key not in ['etag', 'Timestamp']:
                        clean_entity[key] = value
                data.append(clean_entity)
            
            # JSON 파일로 저장
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    "export_date": datetime.now().isoformat(),
                    "total_actions": len(data),
                    "actions": data
                }, f, ensure_ascii=False, indent=2)
            
            print(f"✅ JSON 파일로 내보내기 완료: {filename}")
            
        except Exception as e:
            print(f"❌ JSON 내보내기 실패: {e}")
    
    def get_statistics(self, entities):
        """통계 정보 표시"""
        
        if not entities:
            print("통계를 계산할 데이터가 없습니다.")
            return
        
        print("\n📈 Actions 통계")
        print("="*50)
        
        # 기본 통계
        total = len(entities)
        print(f"총 액션 수: {total}개")
        
        # 우선순위별 통계
        priorities = {}
        for entity in entities:
            priority = entity.get('priority', 'Unknown')
            priorities[priority] = priorities.get(priority, 0) + 1
        
        print(f"\n우선순위별 분포:")
        for priority, count in sorted(priorities.items()):
            percentage = (count / total) * 100
            print(f"  {priority}: {count}개 ({percentage:.1f}%)")
        
        # 타입별 통계
        types = {}
        for entity in entities:
            action_type = entity.get('type', 'Unknown')
            types[action_type] = types.get(action_type, 0) + 1
        
        print(f"\n타입별 분포:")
        for action_type, count in sorted(types.items()):
            percentage = (count / total) * 100
            print(f"  {action_type}: {count}개 ({percentage:.1f}%)")
        
        # 신뢰도 통계
        confidences = [entity.get('confidence', 0) for entity in entities if entity.get('confidence')]
        if confidences:
            avg_confidence = sum(confidences) / len(confidences)
            max_confidence = max(confidences)
            min_confidence = min(confidences)
            
            print(f"\n신뢰도 통계:")
            print(f"  평균: {avg_confidence:.3f}")
            print(f"  최대: {max_confidence:.3f}")
            print(f"  최소: {min_confidence:.3f}")


def main():
    """메인 함수"""
    
    try:
        viewer = ActionsViewer()
        
        while True:
            print("\n" + "="*60)
            print("📊 Azure Table Storage Actions 조회")
            print("="*60)
            print("1. 전체 액션 조회 (테이블)")
            print("2. 전체 액션 조회 (상세)")
            print("3. 담당자별 조회")
            print("4. 우선순위별 조회")
            print("5. 통계 보기")
            print("6. CSV로 내보내기")
            print("7. JSON으로 내보내기")
            print("0. 종료")
            
            choice = input("\n선택하세요 (0-7): ").strip()
            
            if choice == "0":
                print("👋 프로그램을 종료합니다.")
                break
                
            elif choice == "1":
                entities = viewer.get_all_actions()
                viewer.display_actions_table(entities)
                
            elif choice == "2":
                entities = viewer.get_all_actions()
                viewer.display_actions_detailed(entities)
                
            elif choice == "3":
                assignee = input("담당자 이메일 입력 (예: jihoon.park@techcorp.com): ").strip()
                if assignee:
                    entities = viewer.get_actions_by_assignee(assignee)
                    viewer.display_actions_table(entities)
                else:
                    print("담당자를 입력하세요.")
                    
            elif choice == "4":
                priority = input("우선순위 입력 (High/Medium/Low): ").strip()
                if priority:
                    entities = viewer.get_actions_by_priority(priority)
                    viewer.display_actions_table(entities)
                else:
                    print("우선순위를 입력하세요.")
                    
            elif choice == "5":
                entities = viewer.get_all_actions()
                viewer.get_statistics(entities)
                
            elif choice == "6":
                entities = viewer.get_all_actions()
                filename = input("CSV 파일명 입력 (기본: actions_export.csv): ").strip()
                if not filename:
                    filename = "actions_export.csv"
                viewer.export_to_csv(entities, filename)
                
            elif choice == "7":
                entities = viewer.get_all_actions()
                filename = input("JSON 파일명 입력 (기본: actions_export.json): ").strip()
                if not filename:
                    filename = "actions_export.json"
                viewer.export_to_json(entities, filename)
                
            else:
                print("잘못된 선택입니다. 0-7 사이의 숫자를 입력하세요.")
    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")


if __name__ == "__main__":
    main()