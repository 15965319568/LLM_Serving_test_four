"""Deterministic CPU tokenizers used by backend integration tests.

Production drivers can replace this boundary with the model's real tokenizer.
Token identity includes the tokenizer revision; these counts are not claims
about any commercial model's token accounting.
"""
def encode(text, tokenizer):
    if tokenizer == 'unicode-v1':
        return tuple(ord(ch) for ch in text)
    if tokenizer == 'utf8-v1':
        return tuple(text.encode('utf-8'))
    raise ValueError('unknown tokenizer')
