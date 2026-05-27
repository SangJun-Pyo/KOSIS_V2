# 설치 체크리스트

다른 PC에서 [KOSIS_V2](C:\Users\sangj\Kosis-main\KOSIS_V2)을 처음 실행할 때 확인할 항목입니다.

## 1. 기본 준비

1. Windows PC
2. 인터넷 연결
3. GitHub에서 프로젝트 다운로드

`run_app.bat`가 Python이 없으면 `winget`으로 Python 3.13 설치를 시도합니다.

## 2. 프로젝트 받기

1. GitHub에서 ZIP 다운로드 또는 `git clone`
2. 프로젝트 폴더 열기
3. [run_app.bat](C:\Users\sangj\Kosis-main\KOSIS_V2\run_app.bat) 위치 확인

## 3. API 키 준비

실제 KOSIS 수집 실행에는 `KOSIS_API_KEY`가 필요합니다.

권장 방법. 프로젝트 폴더에 `.env` 파일 생성
```text
KOSIS_API_KEY=여기에_실제_API_KEY
```

대안. 프로젝트 폴더에 `.env.local` 파일 생성
```text
KOSIS_API_KEY=여기에_실제_API_KEY
```

빠른 시작용 템플릿으로 [.env.example](C:\Users\sangj\Kosis-main\KOSIS_V2\.env.example) 파일을 참고할 수 있습니다.

## 4. 첫 실행

1. [run_app.bat](C:\Users\sangj\Kosis-main\KOSIS_V2\run_app.bat) 실행
2. Python이 없으면 자동 설치 시도
3. `requirements.txt` 기준으로 현재 Python 환경에 패키지 자동 설치
3. 브라우저에서 `http://localhost:8501` 열기

## 5. 정상 동작 확인

아래가 보이면 정상입니다.

1. 브라우저 탭 제목: `한국지역고용연구소`
2. 메인 화면 진입
3. 작업 목록 표시

## 6. 자주 보는 문제

### Python이 없다고 나오는 경우

1. `winget` 사용 가능 여부 확인
2. 자동 설치가 실패하면 Python 3.13 이상 수동 설치
3. 다시 [run_app.bat](C:\Users\sangj\Kosis-main\KOSIS_V2\run_app.bat) 실행

### API 키가 없다고 나오는 경우

1. `.env` 또는 `.env.local` 생성
2. 터미널/창을 다시 열고 재실행

### 패키지 설치 실패

1. 인터넷 연결 확인
2. 회사망/보안 정책 확인
3. PowerShell 또는 CMD를 관리자 권한으로 다시 실행

## 7. 참고

1. `.deps/`, `.deps_clean/` 같은 로컬 의존성 흔적 폴더는 배포에 포함하지 않습니다.
2. 생성 파일은 `output/` 아래에 저장됩니다.
3. `jobs/**/*.json`, `output/**/*.xlsx`는 하위 폴더까지 자동 탐색합니다.
