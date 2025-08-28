def _ocr_text_from_bytes(img_bytes: bytes) -> str:
    """呼叫 Google Vision OCR → 先試 document_text_detection，若抓不到再用 text_detection。"""
    if not (OCR_ENABLED and VISION_CLIENT):
        return ""
    try:
        image = vision.Image(content=img_bytes)

        # 1️⃣ 先用 document_text_detection（適合表格、印刷文字）
        resp = VISION_CLIENT.document_text_detection(image=image)
        if resp.error.message:
            app.logger.error(f"[OCR] Vision error: {resp.error.message}")
            return ""

        text = ""
        if resp.full_text_annotation and resp.full_text_annotation.text:
            text = resp.full_text_annotation.text.strip()

        # 2️⃣ 如果 document_text_detection 抓不到 → 改用 text_detection（適合手寫/數字）
        if not text:
            resp2 = VISION_CLIENT.text_detection(image=image)
            if resp2.error.message:
                app.logger.error(f"[OCR] Vision error (text_detection): {resp2.error.message}")
                return ""
            if resp2.text_annotations:
                text = resp2.text_annotations[0].description.strip()

        return text
    except Exception as e:
        app.logger.error(f"[OCR] OCR 執行失敗：{e}")
        return ""
