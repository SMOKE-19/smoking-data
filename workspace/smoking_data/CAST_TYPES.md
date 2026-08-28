# Cast 허용 타입

`type_casts`와 0201 payload cast에서 사용할 수 있는 타입 계약이다.

| 타입 | canonical 의미 |
| --- | --- |
| `TEXT`, `STRING` | UTF-8 문자열 |
| `TINYINT`, `INT8` | 8-bit 정수 |
| `SMALLINT`, `INT16` | 16-bit 정수 |
| `INT32`, `INTEGER` | 32-bit 정수 |
| `INT64`, `BIGINT` | 64-bit 정수 |
| `FLOAT`, `FLOAT32` | 32-bit 부동소수점 |
| `REAL` | 32-bit 부동소수점 |
| `FLOAT64`, `DOUBLE` | 64-bit 부동소수점 |
| `BOOL`, `BOOLEAN` | 불리언 |
| `DATE` | 날짜 |
| `TIME` | 시간 |
| `DATETIME` | 날짜·시간 |
| `TIMESTAMP` | 날짜·시간 |
| `DURATION` | 기간 |
| `DECIMAL(p,s)` | 정밀도·scale이 지정된 십진수 |

`INTEGER`는 `INT32`의 alias다. `TIMESTAMP`는 `DATETIME`과 같은 날짜·시간 타입이다.
64-bit 정수가 필요하면 `INT64` 또는 `BIGINT`를 사용한다.

실행 시 입력 칼럼의 Arrow 타입이 목표 타입과 같으면 cast를 생략한다. 동일한 cast를
여러 번 요청해도 동일한 의미의 중복 cast는 생략된다. 입력 타입과 목표 타입이 다를
때만 변환을 수행한다. Python API에서 `stats`를 전달한 경우 생략 횟수는
`skipped_same_dtype`로 확인할 수 있다.
