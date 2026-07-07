# trino2trino 업스트림 PR 준비 — 최종 정리

- 작성일: 2026-07-04, 최종 수정: 2026-07-07 / 기준 커밋: `451a9c2`
- 목표: 첫 PR은 "완벽"이 아니라 **"아키텍처 깔끔 + 깨진 곳 없음"**.
  과한 구현·fallback·정책은 오히려 감점이므로 기능 추가가 아니라 군살 제거로 다듬는다.
- 스코프: delegation 포함 **풀 기능**으로 간다 (분리하지 않는다).

---

## 1. 결론

**판정: 방향 유효, 상태는 Needs work.**

아키텍처(JDBC 기반, read-only 우선)는 Trino 메인테이너(mosabua)가 작성한 roadmap 이슈
[#21791](https://github.com/trinodb/trino/issues/21791)의 제안과 일치한다
("Connector is a simple JDBC connector that exposes a secondary Trino as another data source").
실사용 사례([#29097](https://github.com/trinodb/trino/discussions/29097))는 보조 근거.

PR 제출 전에 필요한 작업은 세 가지다.

1. **버그 수정** — 6건 + 복합 상수 게이트 1건. 리뷰어가 먼저 발견하면 신뢰 손실 (1단계)
2. **군살 제거** — STRICT 모드, 정책성 검증, 죽은 코드 (2단계)
3. **테스트 계약 복원** — 업스트림 프레임워크 테스트 abort는 반려 사유 (3단계)

---

## 2. 작업 순서

### 1단계 — 버그 수정 (재현 테스트를 먼저 작성)

| # | 문제 | 위치 | 확정된 수정 |
|---|------|------|------------|
| 1 | native `time` 컬럼의 WHERE 조건이 `"TIME '...'"` **문자열**로 바인딩됨 → 원격에서 `time = varchar` 타입 에러 추정 | TrinoReadMappingFactory.java:134-138 | 재현 확인 후 `StandardColumnMappings.timeWriteFunction`으로 교체 (toWriteMapping은 이미 올바른 방식 사용 중 — TrinoClient.java:561) |
| 2 | 조인 푸시다운 시 transport 변환(CAST 래핑)이 조인 소스와 최종 스캔에서 **두 번** 적용 → JSON transport 컬럼이면 원격 에러 추정 | TrinoClient.java:406-425, 690-718 | transport projection을 **스캔 시점(`split` 존재 시)에만** 적용. 전제(플래닝=split 없음, 스캔=split 있음)는 base-jdbc 481 바이트코드로 확인 완료 |
| 3 | SQL 주석 마커 `/* RemoteTrinoQuery[...] */`를 제어 판별에 사용 — 사용자 SQL이 같은 문자열을 포함하면 오분류 | TrinoClient.java:720-782 | #2 수정 후 마커의 유일한 용도(마킹 자신)가 사라지므로 **통째 삭제**. EXPLAIN 마커 assert는 이미 병행 존재하는 plan-shape assert로 대체(TestTrinoConnectorIntegration.java:645-647), docs trino.md의 마커 문서화도 제거 |
| 4 | 원격 capabilities 로드 실패가 **영구 캐시**됨 → 일시 장애가 재시작 전까지 delegation 비활성화 | TrinoClient.java:733-758 | 실패는 캐시하지 않되 마지막 실패 후 수 초 **백오프** (volatile 타임스탬프 + 테스트용 Ticker). 그 이상은 만들지 않는다 |
| 5 | `statistics.enabled` 설정이 바인딩만 되고 무시됨 | TrinoClientModule.java:54 | `JdbcStatisticsConfig` 주입 후 `getTableStatistics`에서 체크 |
| 6 | passthrough 핸들 생성이 광범위 catch로 에러를 삼키고 재시도 | TrinoClient.java:434-449 | 바깥쪽 blanket catch를 예외 타입 한정으로 축소 + 원인 예외 보존. **폴백 재시도 구조는 유지**(삼킴만 제거) — 폴백 제거(DESCRIBE OUTPUT 단일 경로화)는 동등성 테스트(익명 컬럼·중복 alias·타입 표현 차이) 갖춘 뒤 별도 진행 |
| 7 | 렌더러가 복합 타입 상수를 타입 게이트 없이 `QueryParameter`로 방출 — 핸들 없는 파라미터의 유일한 바인딩 경로는 `toWriteMapping`이고 복합 타입에서 throw → 위임되면 스캔 타임 쿼리 실패 | TrinoRemoteSqlRenderer.java:243-247, TrinoClient.java:583 | 렌더러 상수 렌더링에 복합 타입 게이트 추가(→ 로컬 폴백). **수정 자체는 repro 결과와 무관하게 확정** — 복합 파라미터는 바인딩 가능한 경로가 없음. repro는 분류만 결정(발화 시 버그, 잠복 시 하드닝). repro는 predicate(`contains(ARRAY[1,2,3], col)`)와 projection(`concat(arr_col, ARRAY[1,2])`) 두 경로 모두 |

### 2단계 — 군살 제거 (기능은 유지, 정책·죽은 코드만 삭제)

- **STRICT 모드 삭제 → mode enum 자체 제거** — 선택적 최적화 실패를 쿼리 실패로 승격시키는
  이례적 시맨틱. STRICT를 지우면 남는 AUTO/OFF는 `enabled` 불리언과 중복이므로
  `TrinoRemoteDelegationMode`·`mode` config·세션 프로퍼티를 통째로 삭제하고
  `remote-delegation.enabled` 하나만 남김(기본 true), `@LegacyConfig`도 제거.
  이때 `Decision` enum·`ExpressionAnalysis`/`ProjectionAnalysis` record·`reason` 필드는
  소비자가 사라지므로 함께 삭제하고 analyzer가 `Optional`을 직접 반환하게 축소.
  문서·샘플·테스트 정리 포함 — README.md(:66, :113), trino.md(mode 설정 행·delegation modes
  목록·세션 프로퍼티 문단), conf/trino/catalog/trino.properties:7,
  TestTrinoDelegationAnalyzer·TestTrinoRemoteSqlRenderer의 mode 참조.
  A2(마커 삭제) 이후에 진행. 단, README transport 표의 `UNSUPPORTED`(타입 처리 용어)는
  별개 — 건드리지 않음
  (TrinoRemoteDelegationConfig.java, TrinoDelegationAnalyzer.java:101-107, TrinoClient.java:827-839)
- **passthrough 정책 축소** — 카탈로그 격리·중첩 `system.query` 거부(정책)는 제거.
  top-level `Query`(SELECT/WITH/VALUES/TABLE)만 허용하는 가드는 **유지** — DML/DDL/CALL뿐
  아니라 SHOW/EXPLAIN 등 비-Query 문장 전부 거부(`instanceof Query`,
  PassthroughCatalogEnforcer.java:45). 정책 추가가 아니라 업스트림 공용 테스트가 강제하는
  계약의 명시적 구현. trino-parser 의존성은 남으며 PR 논의 포인트로 표기.
  README/docs는 PG 관례 문구로 수정 — "top-level Query만 허용, 그 이상의 validation/security
  check는 수행하지 않으며 경계는 원격 접근 제어" + 1:1 카탈로그 매핑은 기본 경로의 계약이고
  passthrough는 명시적 탈출구임을 한 줄로. 기존 거부 테스트는 같은 커밋에서 처분 — raw cross-catalog 참조
  (TestTrinoConnectorIntegration.java:418)는 **성공 테스트로 전환**, cross-catalog·중첩
  table function(TestTrinoConnectorTest.java:466, 480)은 **삭제**(원격 tpch/memory에
  `system.query` 부재로 성공 불가, 원격 에러 전달은 `testNativeQueryIncorrectSyntax`와 중복).
  **결정 근거·기각된 대안·후속 카드는 부록 B 참조**
  (PassthroughCatalogEnforcer.java, README.md:142)
- **죽은 코드 삭제 (도달성 검증 완료 — 부록 A)** —
  ① 빈 `MINIMUM_FUNCTION_VERSION` 버전 게이트는 `isVersionAtLeast()`·`LEADING_VERSION_NUMBER`·
  해당 테스트(TestTrinoCompatibilityRegistry.java:90-95)까지 함께 삭제.
  단 `capabilities.version()` 필드는 원격 버전 로그(TrinoClient.java:743)가 소비하므로 유지.
  ② allowlist의 `if`/`try`/`coalesce` 삭제 확정 — if/try는 번역기에 visit 자체가 없고,
  coalesce는 `$coalesce`로 내려와 `"coalesce"` 문자열과 매칭 불가. 셋 다 도달 불가.
  coalesce 실지원은 PR 이후 카드(4절).
  ③ 복합 타입 write function(`JdbcComplexValueCodec.toJdbcValue`)은 rejecting stub 확정 —
  유일한 도달 경로(튜플 도메인 바인딩)가 `DISABLE_PUSHDOWN`으로 차단됨
- **렌더러·rewriter 이중 구조는 유지**(1단계 7번의 상수 게이트 추가와는 별개) — 표준 rewriter는
  delegation 끔(`enabled=false`) 시맨틱과
  집계 폴백(TrinoClient.java:166의 `AggregateFunctionRewriter` 의존성)이라 제거 불가.
  계층 주석 한 단락만 추가: *renderer = Trino-native 확장 경로, rewriter = baseline(delegation 끔·집계용)*

### 3단계 — 테스트 계약 복원

- **local 러너만** 기본 워커 수로 변경 + `ensureDistributedQueryRunner` abort 삭제
  (TestTrinoConnectorTest.java:708-713, TrinoQueryRunner.java:171-174).
  remote 러너(TrinoQueryRunner.java:85-87)는 워커 0 유지 — 테스트 비용
- `testDataMappingSmokeTest` — abort override(TestTrinoConnectorTest.java:508-515)는 base의
  `skipTestUnless(SUPPORTS_CREATE_TABLE)` skip과 중복이라 삭제(바이트코드 확인).
  base 테스트는 커넥터 경유 CREATE라 원격 fixture로 우회 불가. 잃는 커버리지는 새 클래스
  신설 대신 **기존 `TestTrinoTypeMapping`(68개 테스트)에 보강** — 대표 타입 sample/high value
  SELECT + predicate 케이스. 테스트 클래스 1개당 클러스터 2기 기동이라 신설은 지양.
  같은 메서드명 override로 custom을 넣는 것도 금지(base skip 계약 가림)
- 축소된 베이스 오버라이드 복원 — `newTrinoTable(3-인자, protected non-final)`을
  `onRemoteDatabase()` 원격 fixture 생성으로 override한 뒤 join/aggregation override 삭제.
  aggregation은 `createAggregationTestTable` 훅이 이미 원격 경유라 삭제만으로 복원(확인 완료).
  이 override는 상속 base 테스트 전체의 fixture 경로를 바꾸는 강한 수단 — **적용 후 전체
  테스트 실행으로 triage 필수**. `testArithmeticPredicatePushdown`은 plan 검증 추가.
  남는 오버라이드에는 사유 주석
- **temporal transport 타임존 매트릭스 1개** — UTC + 비정시 오프셋(예: Asia/Kathmandu) +
  DST 존(예: Europe/Warsaw — Bahia_Banderas는 2022년 DST 폐지로 현재 날짜 기준 부적합).
  문자열 파싱으로 timestamp를 복원하는 구조의 최소 방어선. predicate 바인딩(typed bind) 레그는
  대표 존 1개만 — write 경로는 값이 존/오프셋을 자체 보유해 세션 존 비의존이므로 매트릭스와
  곱하지 않는다. 풀 SqlDataTypeTest 전환은 하지 않는다
- 1단계 수정 각각의 회귀 테스트 + Config 클래스 `ConfigAssertions` 테스트
- passthrough 거부 테스트 보강 — 현재는 에러 메시지만 확인(TestTrinoConnectorTest.java:517-521);
  INSERT/CTAS/DELETE/UPDATE는 **원격 상태 불변**(row 미추가, 테이블 미생성) assert 추가.
  **CALL은 분리** — 관찰 가능한 원격 procedure가 없어 상태 불변 assert가 공허하므로
  local reject 메시지 확인까지만(선택: `getQueryManager().getQueries()` 순회로 원격 미도달
  assert — SQL 방식은 검사 쿼리가 자기 토큰에 걸려 부적합)
- **`remote_delegation_enabled=false` 테스트 신설** — 현재 delegation-off 경로(baseline rewriter
  폴백)는 커버리지 0. 끈 상태에서 쿼리 정상 동작 + baseline predicate pushdown 유지를
  **plan-shape 검증**으로 확인

### 4단계 — 포팅

- trinodb/trino fork에 `plugin/trino-trino` 배치, root pom·docs 인덱스·CI 등록
  (out-of-tree 빌드용 bootstrap 스크립트류는 소거)
- **이름 확정** (`trino-trino`/`connector.name=trino` vs `trino-remote`) — 되돌리기 비싼
  유일한 결정이므로 트리 진입 전에
- **스타일 일괄 패스 (포팅 직후)** — 신규 커넥터 PR은 전체가 새 코드라 내부 diff noise 개념이
  없고, 트리의 checkstyle이 모듈 전체에 강제되므로 이 시점에 포괄 수행.
  인라인 FQN → static import(TrinoClient.java:558-570 등), 생성자 `requireNonNull` 보강.
  기준은 Trino checkstyle·관례 — 취향성 리팩터링은 제외

### 5단계 — PR

- **#21791을 주 근거로 인용** — JDBC 아키텍처·read-only 스코프가 이슈 제안과 정렬됨을 1문단으로
- 이슈의 "원격 클러스터 전체 노출" 옵션 대비 1:1 카탈로그 매핑으로 좁힌 차이를 한 줄로 명시
- #29097은 "community interest / field feedback" 수준으로만 언급 (endorsement처럼 쓰지 않기)
- Future work 명시: stddev류 집계 푸시다운, 사용자 신원 전파, 쓰기 지원, cross-version 호환,
  렌더러 rule 재구조화

---

## 3. 하지 않기로 확정한 것 (수정안의 over-engineering 방지)

| 안 하는 것 | 이유 |
|-----------|------|
| delegation을 별도 PR로 분리 | 동작하는 풀 기능으로 간다 (사용자 결정) |
| 렌더러 rule 체계 재구조화 | PR 크기·리스크 과다. 유지 + 계층 문서화로 충분, 리뷰어 요구 시 후속 |
| capabilities TTL·백그라운드 갱신·로그 dedup | 백오프 두 줄 요건이면 충분. 기존 코드도 동시 로드 시도를 허용하는 구조 |
| 핸들 상태 모델 추가 | 마커 삭제로 passthrough/delegated 구분 자체가 불필요해짐 |
| 풀 SqlDataTypeTest 전환 | temporal 타임존 매트릭스만. 나머지는 후속 |
| passthrough 에러 코드 세분화 | blanket catch 제거 + 원인 보존까지만 |
| passthrough 가드(비-Query 거부) 제거 | 불가 — Trino 원격에는 "자연 실패"가 없어 업스트림 테스트 계약 위반 (부록 B) |

## 4. PR 이후로 미루는 것

TypeNameParser의 fromSqlType-우선 단순화, `unsupported-type-handling=IGNORE` 테스트,
CI 러너 공유(테스트 클래스 5개가 각각 클러스터 2개 기동), varchar/char min/max 차단 사유 정리,
연도 9999 초과·중복 alias 등 엣지 에러 메시지, base-jdbc 공통 설정 문서화(fragment),
coalesce delegation 실지원(`STANDARD_OPERATORS`에 `COALESCE_FUNCTION_NAME` 추가 + 렌더러
`renderFunction(..., "COALESCE")` — NULLIF 선례가 TrinoRemoteSqlRenderer.java:314-316에 있음).

---

## 부록 A: 검증 완료된 사실 (재검토 불필요)

- #21791 작성자는 Trino 메인테이너이며 본문에 "simple JDBC connector" 아키텍처와
  read-only 우선 스코프가 명시되어 있다 — JDBC 선택은 방어 대상이 아니라 인용할 카드
- base-jdbc 481 바이트코드 확인: 공개 `prepareQuery`(조인 소스 합성)는 split=empty,
  `buildSql`(스캔)은 split=present로 protected `prepareQuery` 호출 → split 게이팅의 구조 전제 확정
- 표준 rewriter는 삭제 불가 — delegation 끔(`enabled=false`)의 문서화된 시맨틱이자
  `AggregateFunctionRewriter`의 의존성
- trino-main 481 번역기 바이트코드 확인: Coalesce는 `$coalesce`로 번역됨(allowlist의
  `"coalesce"`와 매칭 불가), If/Case/Try는 visit 메서드 자체가 없음, `$array` 방출 지점은
  `visitIn`(IN-리스트 포장) 하나뿐
- base-jdbc 481 `DefaultQueryBuilder` 바이트코드 확인: 컬럼 mapping write function은 튜플 도메인
  바인딩 전용(→ `DISABLE_PUSHDOWN`이면 도달 불가), JdbcTypeHandle 없는 `QueryParameter`는
  `toWriteMapping` 오버로드로 바인딩
- passthrough도 `JdbcQueryRelationHandle`이므로 "query relation에는 transport 변환 재적용 안 함"
  식의 수정은 passthrough 디코딩을 깨서 불가
- passthrough/delegated 구분의 유일한 기능적 소비자는 마커 부착 자신 — 마커를 지우면 구분도 불필요
- `statistics.enabled`는 소스 전체에서 사용처 없음 (grep 확인)
- `NumberType`/`TrinoNumber`는 Trino 481 SPI에 실재하는 공식 타입 (환각 아님)

---

## 부록 B: passthrough 가드 — 결정과 근거 (재논쟁 방지용)

**결정**: 카탈로그 격리·중첩 `system.query` 거부는 제거. 비-Query 문장 거부 가드는 유지.

**근거**:

1. 업스트림 `system.query`가 실제로 보장하는 건 하나뿐 — "plain INSERT/CREATE는 실패 + 부수효과
   없음". `BaseJdbcConnectorTest`가 이를 직접 assert함(실패 확인 후 원격 상태 불변까지 — 바이트코드
   확인). 의미론적 read-only는 어떤 커넥터도 보장 안 함(PG의 `DELETE ... RETURNING`은 실행됨.
   공식 문서: "Trino does not perform these tasks").
2. 다른 커넥터는 이 계약을 공짜로 지킴 — 벤더 드라이버가 INSERT에 `getMetaData() = null`을 주면
   base-jdbc가 실행 전에 거부. 원격이 Trino면 INSERT도 결과 모양(`rows`)이 있어 null이 아님 →
   자연 실패 없음 → 가드가 없으면 INSERT가 실제 실행됨.
3. 따라서 가드 = 남들이 드라이버에게 공짜로 받는 계약의 명시적 구현. 정책 추가가 아님.

**기각된 대안**:

- 파서 제거 + 원격 자연 실패 의존: 자연 실패가 없음(근거 2) — 검증하면 불합격
- read-only 문구를 SPI 스코프로 낮춰 가드 제거: 문구 문제가 아니라 테스트 계약(근거 1) — 회피 불가
- 카탈로그 격리·중첩 거부 유지: 명시적 이름만 잡는 불완전 울타리(뷰·2-part·타 PTF 통과), 경계는 원격 ACL

**잔존 비용**: trino-parser 의존은 남음(`instanceof Query` 한 곳). 버전 스큐 시 오거부 가능
(fail-closed — 안전한 방향, cross-version 미주장과 정합). PR 논의 포인트로 표기.

**파서 제거 후속 카드**: `SELECT * FROM (sql) t` 무조건 래핑 → 원격 파서가 실행 전에 거부.
전제: F3 단일 경로화 선행 + WITH/VALUES/TABLE 래핑 검증. (차선: 첫 키워드 allowlist)

**문서 문구**: 내부 기준은 "문장 수준 쓰기는 차단, 원격 측 부수효과는 미통제, 실행 경계는 원격
접근 제어". 공개 문서(README/trino.md)는 PG 관례를 따라 "top-level Query만 허용하며, Trino는
그 이상의 validation/security check를 수행하지 않고 경계는 원격 접근 제어"로 표기 — 의미는
같되 위험 신호로 읽히는 직설 표현 회피. 우리의 원격 PTF 구멍 = PG의 `RETURNING` 구멍 —
업스트림과 동일한 위치. ("best-effort" 표기는 과소표현이라 기각 — 검사는 자기 스코프 안에서 결정적)
