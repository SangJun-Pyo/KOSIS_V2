# KOSIS 웹앱 구현 에이전트 브리프 (Agent용)

## 역할
당신은 이 저장소에서 작업하는 구현 에이전트다.  
목표는 `runner.py` 중심 배치 프로젝트를 운영 가능한 웹앱으로 전환하는 것이다.

## 작업 원칙
- 기존 `runner.py` 비즈니스 로직은 우선 재사용
- 큰 리라이트보다 작은 단위로 안전하게 전진
- 각 단계마다 실행 가능 상태를 유지
- 실패 시 원인과 재현 경로를 로그에 남김

## 현재 프로젝트 구조 (요약)
- 핵심 실행기: `runner.py`
- 잡 정의: `jobs/**/*.json`
- 산출물: `output/**/*.xlsx`
- 실행 스크립트: `run_jobs.bat`, `run_all.bat`, `run_*.bat`
- UI 초안: `app.py` (Streamlit)

## 1차 목표 (MVP)
- Streamlit UI에서 실제 실행 연결
- 잡 선택(전체/카테고리/개별) 지원
- 실행 로그 실시간 표시
- 결과 파일 목록/다운로드 제공

## 작업 백로그

### Task A. 실행 서비스 레이어 분리
- `runner.py` 호출을 직접 UI에 박지 말고, 얇은 서비스 모듈로 래핑
- 예시 파일: `services/job_runner_service.py`
- 책임:
  - 입력 파라미터 표준화
  - 실행 함수 호출
  - 예외를 표준 오류 객체로 변환

### Task B. Streamlit UI-실행 연결
- `app.py` 버튼 이벤트를 서비스 레이어와 연결
- 상태 표시:
  - `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`
- 실행 중 중복 클릭 방지

### Task C. 로그 스트림
- 최소 요구:
  - 시간, 레벨, 메시지
  - 최근 N줄 유지(예: 500줄)
- UI 필터(레벨/키워드) 제공

### Task D. 결과 파일 브라우저
- `output/` 하위 xlsx 목록 표시
- 파일명, 수정일, 크기 표시
- 다운로드 버튼 제공

### Task E. 설정 관리
- 타임아웃/재시도 등 UI 값과 실행 파라미터 연동
- API 키는 평문 노출 금지(마스킹)

## 완료 기준 (Definition of Done)
- 로컬에서 `run_app.bat` 실행 시 웹 UI 정상 접속
- 실제 잡 실행 후 `output/` 파일 생성 확인
- 실패 케이스에서 UI에 오류 메시지 표시
- README에 실행 방법과 제한사항 반영

## 제안 폴더 구조
```text
services/
  job_runner_service.py
ui/
  components.py
  state.py
app.py
```

## 기술 부채 메모
- `runner.py`가 단일 파일로 크므로 점진적 모듈화 필요
- 인코딩/콘솔 출력 처리(한글, ANSI) 정리 필요
- 실행 이력 저장(DB) 도입은 2단계에서 진행

## 커밋 전략
- 한 Task당 1커밋 원칙
- 커밋 메시지 예시:
  - `feat(ui): connect streamlit run buttons to runner service`
  - `feat(log): add in-app run log stream and filter`
  - `feat(results): add output file browser and download`

## 에이전트 출력 템플릿
작업 완료 시 아래 형식으로 보고:

1. 변경 요약
2. 변경 파일 목록
3. 실행/검증 결과
4. 남은 리스크
5. 다음 권장 작업 3개
