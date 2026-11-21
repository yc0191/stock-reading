package dev.stockreading.notifier.event;

/** 이벤트가 싣는 인라인 그림의 공통 모양. data는 base64 문자열이다. */
public interface ImageData {

    String contentId();

    String contentType();

    String data();
}
