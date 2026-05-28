# KOSIS_V2 Repository Structure Guide

이 문서는 `KOSIS_V2` 레포의 주요 기능 구조와 폴더 역할을 빠르게 파악하기 위한 안내서입니다.

## 1. 프로젝트 목적

이 레포는 KOSIS/OpenAPI 기반 통계 수집 작업을

- `jobs/*.json` 작업 정의로 관리하고
- `runner_core` 엔진으로 실행하며
- Streamlit 대시보드에서 실행/모니터링/결과 확인

할 수 있도록 구성된 운영형 프로젝트입니다.

핵심 목표는 다음과 같습니다.

1. 작업 정의를 코드 수정 없이 JSON으로 추가
2. KOSIS 원천 데이터를 표 형태 결과물로 자동 가공
3. 운영자가 대시보드에서 작업 실행/재실행/로그 확인
4. 결과물을 `output/` 아래에 정리

## 2. 최상위 구조

```text
KOSIS_V2/
  app.py
  run_app.bat
  runner.py
  requirements.txt
  .env.example
  jobs/
  output/
  runner_core/
  services/
  state/
  ui/
  scripts/
  .streamlit/
```

## 3. 실행 진입점

### `run_app.bat`
- Windows 사용자용 실행 진입점
- Python 존재 여부 확인
- 필요 시 `winget`으로 Python 설치 시도
- `pip` 없으면 `ensurepip`로 복구 시도
- 의존성 설치 후 Streamlit 대시보드 실행

### `app.py`
- Streamlit 앱 메인 진입점
- 화면 구성은 주로 `ui/`, 상태 관리는 `state/`, 실행 로직은 `services/`에 위임

### `runner.py`
- 기존 작업 실행 흐름과 호환되는 배치 실행 진입점
- JSON job을 직접 실행할 때 사용하는 기본 경로

## 4. 핵심 폴더 설명

### `jobs/`
실제 통계 작업 정의가 들어 있는 폴더입니다.

분야별 하위 폴더:
- `population/`
- `economy_industry/`
- `employment_labor/`
- `tourism_culture/`

각 분야는 다시 보통 아래처럼 나뉩니다.
- `common/`: 전국/광역/시도 공통 비교형 작업
- `incheon/`: 인천 전용 작업
- `multi_region/`: 특정 복수 지역 비교 작업

각 JSON 파일은 보통 아래를 포함합니다.
- KOSIS 원천 정보 (`orgId`, `tblId`, `itmId`, `objL*`, 기간 등)
- provider 유형 (`kosis_sources`, `kosis_multi`)
- 전처리 규칙
- TABLE_VIEW 생성 방식
- output 저장 위치
- scope 메타 정보

### `output/`
- 실행 결과 엑셀 파일 저장 위치
- `jobs/`와 유사한 폴더 구조를 따르는 것이 권장됨

### `runner_core/`
이 프로젝트의 실행 엔진입니다.

주요 역할:
- KOSIS API 호출
- source 병합
- 필터링 / 전처리 / 값 치환
- 피벗 생성
- TABLE_VIEW 생성

하위 구성 개념:
- `api/`: HTTP/KOSIS API 호출
- `providers/`: job provider 실행기
- `preprocess/`: 전처리/치환/집계 변환
- `pivots/`: 표 형태 생성 로직
- `views/`: source 기반 TABLE_VIEW 조립

### `services/`
앱 서비스 계층입니다.

대표 역할:
- 작업 목록 파싱
- 실행 상태 관리
- 결과 파일 목록 수집
- 로그/진행률 보조

주요 파일:
- `job_catalog_service.py`
- `job_runner_service.py`

### `state/`
- Streamlit session state 초기화 및 보정
- 실행 화면에서 필요한 상태값 관리

### `ui/`
- 대시보드 UI 렌더링 담당
- 화면 배치, 스타일, 실시간 패널, 버튼/결과 영역 포함

### `scripts/`
- 점검/회귀/보조 실행 스크립트

### `.streamlit/`
- Streamlit 설정 파일 위치

## 5. Provider 구조

이 레포에서 자주 쓰는 provider는 두 가지입니다.

### `kosis_sources`
- source별 raw를 개별적으로 불러옴
- 각 source에 별도 필터/전처리 적용 가능
- source 기반 block/table 구성이 유리함

적합한 경우:
- 원천이 1개 이상
- raw sheet도 source별로 남기고 싶을 때
- block 형태 TABLE_VIEW가 필요한 경우

### `kosis_multi`
- 여러 source를 merge 후 metric 계산
- 파생지표, 비율, 평균, 전국대비 비중 계산에 유리

적합한 경우:
- 전국/인천/광역 평균 같이 합성 지표가 필요한 경우
- formula 기반 계산이 필요한 경우

## 6. TABLE_VIEW 생성 방식

`jobs/*.json`의 `views`에서 출력 형태를 정합니다.

최근 자주 쓰는 kind 예시:
- `single_metric_share_summary`
- `row_timeseries`
- `latest_metric_matrix`
- `dual_label_timeseries_summary`
- `dual_label_latest_compare`
- `region_year_metric_matrix`
- `stack_blocks`

즉, 같은 raw라도 view 구성을 바꾸면 참고자료와 비슷한 결과 표 형태를 만들 수 있습니다.

## 7. 설정 파일

### `.env`
권장 로컬 설정 파일입니다.

예:
```text
KOSIS_API_KEY=YOUR_REAL_API_KEY
```

### `.env.local`
- 대안 설정 파일
- `.env` 다음 단계 보조 설정으로 사용 가능

### `.env.example`
- 초기 세팅용 템플릿

## 8. 배포 시 포함/제외 권장

포함 권장:
- 코드 전체
- `jobs/`
- `requirements.txt`
- `run_app.bat`
- `README.md`
- `INSTALL_CHECKLIST.md`
- `.env.example`

포함 비권장:
- `.env`
- `.env.local`
- `output/`
- `.deps/`
- `.deps_clean/`
- 로컬 참고용 원본 엑셀

## 9. 새 작업 추가 흐름

일반적인 추가 순서:

1. 참고 엑셀 탭 구조 확인
2. KOSIS URL/원천 파라미터 확인
3. 기존 유사 JSON 패턴 찾기
4. `jobs/<분야>/<scope>/`에 새 JSON 작성
5. 필요 시 `runner_core` pivot/preprocess/view 보강
6. live KOSIS 호출로 검증
7. 앱에서 재실행 확인

## 10. 운영 관점에서 보면 좋은 파일

처음 볼 때 추천 순서:

1. `run_app.bat`
2. `app.py`
3. `services/job_catalog_service.py`
4. `services/job_runner_service.py`
5. `runner_core/providers/`
6. `runner_core/pivots/summary.py`
7. `jobs/` 아래 실제 JSON 예시

## 11. 현재 구조의 장점

- job 추가가 비교적 빠름
- raw/source/view를 분리해서 관리 가능
- 참고 엑셀 형태를 꽤 유연하게 복제 가능
- Streamlit 운영 화면과 배치 실행 자산을 함께 유지 가능

## 12. 현재 구조에서 주의할 점

- KOSIS 원천 구조가 바뀌면 일부 job은 재조정 필요
- JSON이 많아질수록 유사 패턴 정리/공통화가 필요
- source view 실패가 조용히 넘어가지 않도록 검증 루틴 유지 필요
- 참고자료 기준 값과 live KOSIS 최신값이 다를 수 있으므로 검증 날짜 의식 필요

