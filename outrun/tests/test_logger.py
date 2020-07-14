import outrun.logger as logger


def test_summarize_matching_length():
    assert logger.summarize("abc", max_length=3) == "abc"


def test_summarize_exceeding_length():
    assert logger.summarize("abcdef", max_length=5) == "ab..."


def test_summarize_list():
    x = [1, 2, 3, 4, 5]
    assert logger.summarize(x, max_length=6) == "[1,..."
