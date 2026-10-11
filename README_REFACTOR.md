# LINE 커뮤니티 봇 — 구조 리팩토링

## 구성

```text
main.py
linebot_app/
  __init__.py
  commands.py    # 명령어 별칭과 입력 파싱
  database.py    # PostgreSQL 연결 풀, 스키마, 마이그레이션, SQL 어댑터
  line_utils.py  # LINE 이벤트 필드, 멘션 대상, 대화방 식별, UTC 시간
  services.py    # 메시지/입퇴장 저장, 통계, 경고 관련 데이터 작업, 보존 기간 정리
requirements.txt
tests/
```

`main.py`는 FastAPI 앱 생명주기, LINE 웹훅과 명령어 응답 조립을 담당한다. 기존 `uvicorn main:app` 실행 형태를 유지한다.

## 적용 방법

1. 기존 프로젝트를 별도 위치에 백업한다.
2. `main.py`, `requirements.txt`와 `linebot_app/`, `tests/`를 같은 프로젝트 디렉터리에 배치한다.
3. 기존 `.env`는 그대로 사용한다. 새 비밀키를 만들거나 데이터베이스를 새로 생성할 필요가 없다.
4. 기존과 동일하게 의존성을 설치하고 서버를 실행한다.

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

호스팅 플랫폼에서는 기존에 사용하던 시작 명령과 포트 설정을 유지하면 된다.

## 호환성 원칙

- 기존 PostgreSQL 테이블과 기존 마이그레이션을 유지한다.
- LINE 채널 환경변수와 관리자 ID, 소통 기준 관련 환경변수 이름을 유지한다.
- 한국어/영어 명령어 별칭과 기존 응답 문구를 유지한다.
- 멘션/답장 알림의 예약·전송 상태와 웹훅 중복 이벤트 방지 로직을 유지한다.
- DB 풀 생성/종료와 스키마 초기화 코드를 `linebot_app.database`로 이동했다.

## 검증

로컬 Python에서 다음을 실행한다.

```bash
python -m compileall -q main.py linebot_app
python -m unittest discover -s tests -v
```

테스트는 명령어 파싱과 LINE 이벤트 유틸리티를 대상으로 한다. 실제 LINE 웹훅, LINE API 자격 증명, Neon PostgreSQL에 대한 통합 테스트는 운영 환경의 `.env`와 DB 연결이 필요하다.

## 배포 후 확인

1. `/`에서 `{"status":"ok","service":"line-community-bot"}` 응답 확인
2. `/health/db`에서 DB 상태 확인
3. 테스트 대화방에서 `!도움말`, `!통계`, `!입퇴장`, 멘션 알림을 확인
4. 관리자 기능은 `ADMIN_USER_IDS`가 기존과 동일하게 설정되어 있는지 확인

기존 DB를 삭제하거나 초기화하지 않는다.
