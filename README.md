# KOSIS_V1 Renew

`KOSIS_V1`는 기존 `runner.py` 기반 배치 프로젝트를 웹앱 운영 콘솔 형태로 리뉴얼한 버전입니다.

## 포함 내용
- `app.py`: Streamlit 메인 앱
- `services/job_runner_service.py`: 실행 서비스 레이어
- `ui/components.py`, `ui/state.py`: UI 모듈
- `runner.py`, `jobs/`: 원본 실행 자산 재사용

## 실행 방법
1. `run_app.bat` 실행
2. 브라우저에서 `http://localhost:8501` 접속

## 현재 지원 기능
- 카테고리/잡 선택 실행
- 실행 중지
- 실행 로그 표시
- 결과 파일 목록 및 다운로드

## 참고
- API 키는 기존과 동일하게 `KOSIS_API_KEY` 환경변수를 사용합니다.
- 생성 결과물은 `output/`에 저장됩니다.
