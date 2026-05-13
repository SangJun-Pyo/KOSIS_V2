# KOSIS MES 스타일 웹앱 마스터 플랜

## 1. 기획 방향
- 제품 포지션: "통계 수집 MES 운영 콘솔"
- 목표: 배치 실행 도구를 제조 MES처럼 "상태 중심, 알람 중심, 작업 지시 중심"으로 전환
- 기준일: 2026-05-11

## 2. MES 스타일 핵심 원칙
- 실시간성: 현재 어떤 작업이 어디까지 진행 중인지 한눈에 보여준다.
- 표준화: 작업 지시서(Job Order) 단위로 실행/중지/재실행한다.
- 추적성: 누가/언제/무엇을 실행했고 결과가 어땠는지 남긴다.
- 예외관리: 실패 원인과 재처리 버튼을 같은 화면에서 제공한다.

## 3. 사용자 역할
- 운영자(Operator): 일일 실행, 실패 대응, 결과 다운로드
- 관리자(Admin): 환경설정, API 키 관리, 스케줄 정책 관리
- 분석가(Analyst): 결과 확인, 버전 비교, 품질 검토

## 4. 화면 구조 (MES 레이아웃)
- 상단 바(Global Header): 시스템명, 현재 시간, 환경(LOCAL/PROD), 알람 아이콘
- 좌측 사이드바(Navigation): Dashboard / Work Orders / Monitor / Artifacts / Logs / Settings
- 본문 3분할:
  - 좌측 패널: 작업 지시 목록(대기/실행/완료/실패)
  - 중앙 패널: 선택 작업 상세 + 진행률 + 단계별 상태
  - 우측 패널: 실시간 이벤트/알람 피드
- 하단 고정 패널: 로그 콘솔(필터, 검색, 자동 스크롤)

## 5. 정보 아키텍처
1. Dashboard
- 금일 실행 건수, 성공률, 평균 처리시간, 실패 건수
- 카테고리별 상태 히트맵

2. Work Orders
- 신규 작업 생성 (전체/카테고리/개별 잡)
- 우선순위 지정 (P1/P2/P3)
- 즉시 실행/예약 실행

3. Monitor
- 현재 실행 파이프라인 단계 표시
- Job별 진행률, 병목 단계, 예상 완료 시간(ETA)

4. Artifacts
- 산출물 목록, 생성 시각, 크기, 실행ID 매핑
- 다운로드, 이전 버전 비교(차기)

5. Logs
- 실시간 스트림, 레벨별 필터(INFO/WARN/ERROR)
- 실행ID 단위 로그 추적

6. Settings
- API 키, 재시도, 타임아웃, 동시실행 제한
- 알람 룰(실패시 알림 등)

## 6. 도메인 모델 (MES식)
- WorkOrder: 실행 지시 단위
- WorkStep: 실행 단계(Validate -> Fetch -> Transform -> Save -> Verify)
- JobSpec: jobs/*.json 메타
- RunRecord: 실제 실행 이력
- Artifact: output 파일 메타
- EventLog: 구조화 로그

## 7. 상태 모델
- WorkOrder: `DRAFT -> QUEUED -> RUNNING -> COMPLETED | FAILED | CANCELED`
- WorkStep: `PENDING -> RUNNING -> SUCCESS | FAILED | SKIPPED`

## 8. 핵심 UX 시나리오
1. 운영자 아침 루틴
- Dashboard에서 전일 실패 건 확인
- Work Orders에서 실패 잡 재실행
- Monitor에서 진행률 확인
- Artifacts에서 결과 다운로드 후 공유

2. 장애 대응
- 우측 알람 피드에서 ERROR 클릭
- 해당 RunRecord 상세로 이동
- 실패 단계/원인 확인
- "재처리(동일 파라미터)" 버튼 실행

## 9. 구현 전략

### Phase 1: Streamlit MES UI MVP (1~2주)
- 현재 `KOSIS_V1` 기반 고도화
- 3분할 레이아웃 + 하단 로그 콘솔
- WorkOrder 개념(메모리/파일 기반) 도입

### Phase 2: FastAPI 분리 + 상태저장 (2~4주)
- 실행 엔진 API화
- SQLite/PostgreSQL로 RunRecord 저장
- 로그 구조화(JSON) 및 검색

### Phase 3: Next.js 전환 (4~6주)
- MES형 컴포넌트 강화 (그리드, 간트, 알람센터)
- 권한 모델/감사로그/스케줄러 확장

## 10. KPI
- 성공률 95%+
- 실패 인지시간 5분 이내
- 재처리 완료시간 15분 이내
- 운영자 수동 개입 건수 월 30% 감소

## 11. 리스크 및 대응
- API 응답 스키마 변경: 검증 레이어 + fallback 룰
- 장시간 실행 중 UI 세션 종료: 백엔드 큐 분리
- 파일 충돌: 실행ID 기반 파일명 정책

## 12. 바로 다음 실행 항목
1. `KOSIS_V1`의 UI를 MES 3패널 구조로 리팩터링
2. WorkOrder/RunRecord 임시 저장(JSON) 추가
3. 로그 이벤트를 구조화(`timestamp`, `level`, `run_id`, `message`)로 통일
