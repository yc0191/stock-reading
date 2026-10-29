package dev.stockreading.collector.edgar;

/** EDGAR 호출 실패의 종류. 다시 시도할지, 설정을 고칠지 판단하는 데 쓴다. */
public enum EdgarFailure {
    /** User-Agent 가 비어 있다. 호출하지 않는다. */
    NOT_CONFIGURED,
    /** SEC 티커 매핑 파일에 없는 티커다. */
    UNKNOWN_TICKER,
    /** 404. CIK 에 해당하는 공시 이력이 없다. */
    NOT_FOUND,
    /** 403. User-Agent 선언이 없거나 SEC 가 호출 속도 초과로 막았다. */
    FORBIDDEN,
    /** 429. 호출 속도 초과. */
    RATE_LIMITED,
    /** 그 밖의 4xx. */
    CLIENT_ERROR,
    /** 5xx. */
    SERVER_ERROR,
    /** 응답 시간 초과. */
    TIMEOUT,
    /** 연결 실패 등 입출력 오류. */
    NETWORK,
    /** 응답이 JSON 이 아니거나 예상한 구조가 아니다. */
    INVALID_RESPONSE,
    /** 기다리는 중 스레드가 중단되었다. */
    INTERRUPTED
}
