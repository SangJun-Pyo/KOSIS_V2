# 설치 체크리스트

다른 PC에서 [KOSIS_V2](C:\Users\sangj\Kosis-main\KOSIS_V2)을 처음 실행할 때 확인할 항목입니다.

## 1. 기본 준비

1. Windows PC
2. 인터넷 연결
3. Python 3.13 이상 설치
4. GitHub에서 프로젝트 다운로드

## 2. 프로젝트 받기

1. GitHub에서 ZIP 다운로드 또는 `git clone`
2. 프로젝트 폴더 열기
3. [run_app.bat](C:\Users\sangj\Kosis-main\KOSIS_V2\run_app.bat) 위치 확인

## 3. API 키 준비

실제 KOSIS 수집 실행에는 `KOSIS_API_KEY`가 필요합니다.

방법 1. 환경변수 설정
```powershell
setx KOSIS_API_KEY "여기에_실제_API_KEY"
```

방법 2. 프로젝트 폴더에 `.env.local` 파일 생성
```text
KOSIS_API_KEY=여기에_실제_API_KEY
```

## 4. 첫 실행

1. [run_app.bat](C:\Users\sangj\Kosis-main\KOSIS_V2\run_app.bat) 실행
2. `requirements.txt` 기준으로 `.deps/`에 패키지 자동 설치
3. 브라우저에서 `http://localhost:8501` 열기

## 5. 정상 동작 확인

아래가 보이면 정상입니다.

1. 브라우저 탭 제목: `한국지역고용연구소`
2. 메인 화면 진입
3. 작업 목록 표시

## 6. 자주 보는 문제

### Python이 없다고 나오는 경우

1. Python 설치
2. 설치 시 `Add Python to PATH` 체크
3. 다시 [run_app.bat](C:\Users\sangj\Kosis-main\KOSIS_V2\run_app.bat) 실행

### API 키가 없다고 나오는 경우

1. `.env.local` 생성 또는 `KOSIS_API_KEY` 환경변수 설정
2. 터미널/창을 다시 열고 재실행

### 패키지 설치 실패

1. 인터넷 연결 확인
2. 회사망/보안 정책 확인
3. PowerShell 또는 CMD를 관리자 권한으로 다시 실행

## 7. 참고

1. 로컬 의존성은 `.deps/`에 설치됩니다.
2. 생성 파일은 `output/` 아래에 저장됩니다.
3. `jobs/**/*.json`, `output/**/*.xlsx`는 하위 폴더까지 자동 탐색합니다.
