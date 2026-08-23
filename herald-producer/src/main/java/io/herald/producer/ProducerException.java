package io.herald.producer;

/** 生产端异常：网络失败、超时、服务端返回错误等。 */
public class ProducerException extends RuntimeException {

    private final int errorCode;

    public ProducerException(String message) {
        this(message, -1, null);
    }

    public ProducerException(String message, int errorCode) {
        this(message, errorCode, null);
    }

    public ProducerException(String message, Throwable cause) {
        this(message, -1, cause);
    }

    public ProducerException(String message, int errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public int errorCode() {
        return errorCode;
    }
}
