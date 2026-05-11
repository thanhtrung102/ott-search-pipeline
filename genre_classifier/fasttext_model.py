"""
FastText genre classifier — Stage 3 of the waterfall.

Requires a trained model binary at FASTTEXT_MODEL_PATH (default shown below).
Graceful no-op if the model file is absent; rules.py catches ImportError + exceptions.

Training (one-time, SageMaker or local GPU):
    import fasttext
    model = fasttext.train_supervised(
        input="genre_train.txt",   # __label__GENRE keyword_norm, one per line
        epoch=25, lr=0.5, wordNgrams=2, dim=100,
    )
    model.save_model("genre_model.bin")

Upload the .bin to S3 and add it to Glue extra-py-files or the Lambda Layer.
"""
import os

_model = None


def _load_model():
    global _model
    if _model is None:
        path = os.environ.get("FASTTEXT_MODEL_PATH", "/opt/python/genre_model.bin")
        import fasttext  # noqa: PLC0415 — intentional lazy import
        _model = fasttext.load_model(path)
    return _model


def predict_genre(kw: str) -> str:
    """Return genre label if confidence >= 0.65, else 'UNKNOWN'."""
    try:
        m = _load_model()
    except Exception:
        return "UNKNOWN"
    labels, probs = m.predict(kw, k=1)
    if probs[0] >= 0.65:
        return labels[0].replace("__label__", "")
    return "UNKNOWN"
