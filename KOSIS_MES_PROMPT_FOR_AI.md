# KOSIS MES 스타일 UI/기획 생성 프롬프트 (AI 전달용)

당신은 MES/운영콘솔 설계 경험이 있는 시니어 프로덕트 디자이너 + 솔루션 아키텍트다.  
다음 조건으로 KOSIS 데이터 수집 시스템의 웹앱 리뉴얼안을 작성하라.

## 컨텍스트
- 기존 프로젝트는 `runner.py` + `jobs/*.json` + `output/*.xlsx` 구조다.
- 현재는 Streamlit 기반의 초기 UI가 존재한다.
- 목표는 "MES 같은 운영 프로그램" 느낌의 정보 밀도 높은 대시보드다.
- 기준일: 2026-05-11

## 요구사항
1. 기획 산출물
- 제품 목표/비목표
- 사용자 역할(운영자/관리자/분석가)
- 핵심 시나리오(일일 운영, 장애 대응, 재실행)

2. IA 및 화면 설계
- 필수 메뉴: Dashboard, Work Orders, Monitor, Artifacts, Logs, Settings
- 3패널 레이아웃 + 하단 로그 콘솔 구조 제안
- 화면별 컴포넌트 명세(테이블 컬럼 포함)

3. 상태/도메인 모델
- WorkOrder, WorkStep, RunRecord, Artifact, EventLog 정의
- 상태 전이:
  - WorkOrder: DRAFT -> QUEUED -> RUNNING -> COMPLETED|FAILED|CANCELED
  - WorkStep: PENDING -> RUNNING -> SUCCESS|FAILED|SKIPPED

4. 기술 제안
- 단기: Streamlit 고도화
- 중기: FastAPI 분리 + DB 저장
- 장기: Next.js 전환
- 각 단계별 장단점과 전환 기준 제시

5. 운영 관점
- 장애 알람 룰
- 로그 표준 스키마
- 재시도 정책
- 감사로그 최소 항목

## 출력 형식
- 한국어 Markdown
- 섹션: `기획`, `화면`, `데이터모델`, `아키텍처`, `로드맵`, `운영정책`
- 마지막에 "즉시 실행 체크리스트 10개" 제공

## 톤
- 실무자 인수인계 문서처럼 명확하고 실행 가능하게 작성
