from b24api.query import build_query


def test_build_query_simple() -> None:
    query = build_query({"foo": 1, "bar": None, "rest": "abc"})
    assert query == "foo=1&rest=abc"


def test_build_query_empty() -> None:
    query = build_query({"none": None, "empty": "", "zero": 0, "list": []})
    assert query == "empty=&zero=0"


def test_build_query_list() -> None:
    query = build_query({"select": ["ID", "TITLE"]})
    assert query == "select%5B0%5D=ID&select%5B1%5D=TITLE"


def test_build_query_deep() -> None:
    query = build_query(
        {
            "user": {
                "name": "Bob Smith",
                "age": 47,
                "sex": "M",
                "dob": "5/12/1956",
            },
            "pastimes": ["golf", "opera", "poker", "rap"],
            "children": {
                "bobby": {"age": 12, "sex": "M"},
                "sally": {"age": 8, "sex": "F"},
            },
            "0": "CEO",
        },
    )
    expected = (
        "user%5Bname%5D=Bob+Smith&user%5Bage%5D=47&user%5Bsex%5D=M"
        "&user%5Bdob%5D=5%2F12%2F1956&pastimes%5B0%5D=golf"
        "&pastimes%5B1%5D=opera&pastimes%5B2%5D=poker"
        "&pastimes%5B3%5D=rap&children%5Bbobby%5D%5Bage%5D=12"
        "&children%5Bbobby%5D%5Bsex%5D=M"
        "&children%5Bsally%5D%5Bage%5D=8"
        "&children%5Bsally%5D%5Bsex%5D=F&0=CEO"
    )
    assert query == expected
