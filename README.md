# KOSIS_V1 Renew

`KOSIS_V1`는 기존 `runner.py` 기반 배치 프로젝트를 Streamlit 운영 콘솔 형태로 리뉴얼한 버전입니다.

## 포함 내용
- `app.py`: 페이지 조립 전용 진입점
- `services/job_runner_service.py`: 실행/중지/로그/결과 파일 수집
- `services/job_catalog_service.py`: jobs 메타 파싱, 지역 분류, 항목 필터링
- `state/dashboard_state.py`: session state 초기화, 실행 매트릭스, 완료 상태 보정
- `ui/app_styles.py`: 공통 스타일
- `ui/dashboard_views.py`: 지역 선택, 작업 선택, 실시간 패널 렌더링
- `runner.py`, `jobs/`: 원본 실행 자산

## 실행 방법
1. `run_app.bat` 실행
2. 브라우저에서 `http://localhost:8501` 접속

## 현재 지원 기능
- 지역 선택 + 파트/항목 선택 실행
- 실패 작업 재실행
- 실행 중지
- 실시간 상태/KPI/로그/결과 파일 갱신
- 결과 파일 다운로드

## 참고
- API 키는 `KOSIS_API_KEY` 환경변수를 사용합니다.
- 생성 결과물은 `output/` 아래에 저장됩니다.
- 로컬 의존성은 `.deps/`에 설치되며 git에는 포함하지 않습니다.
