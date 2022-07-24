import os
import pytest

from utils import get_utc_time_now
from persistence import StorageSystem


@pytest.fixture
def mongo_connection_string():
    return os.environ["MONGO_DB_CONNECTION_STRING"]


@pytest.mark.parametrize(
    "event_name,message_data,is_supported",
    [
        (
            "Transfer",
            {
                "event_name": "Transfer",
                "filter_arguments": {"fromBlock": "latest"},
                "event_data": {
                    "args": {
                        "from": "0x7b88944595a396af405225411c6D8F3Ad1285f53",
                        "to": "0x3cD751E6b0078Be393132286c442345e5DC49699",
                        "value": 43815937800485491278,
                    },
                    "event": "Transfer",
                    "logIndex": 318,
                    "transactionIndex": 188,
                    "transactionHash": "0xc34e2eb0259d32458408663537419fde07a3fc95cfcf775c93565f7c343403a6",
                    "address": "0x3845badAde8e6dFF049820680d1F14bD3903a5d0",
                    "blockHash": "0x1013aa058d59d32ba4814f14485a5996f9bd25bd1ee034277ec0f64e03f7f8d5",
                    "blockNumber": 15201691,
                },
            },
            False,
        ),
        (
            "PairCreated",
            {
                "event_name": "PairCreated",
                "filter_arguments": {"fromBlock": "latest"},
                "event_data": {
                    "args": {
                        "token0": "0xb4Dc819D42226BB5A96235416Ef48756037Fe996",
                        "token1": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                        "pair": "0x47C84435A23a586ac615513A8Fe87ac6A564eA23",
                        "": 82239,
                    },
                    "event": "PairCreated",
                    "logIndex": 320,
                    "transactionIndex": 278,
                    "transactionHash": "0xaa617c7e680d17b3cdd12eb8249893107e5a2b0a396fb56e4b61a7f6d8ac2396",
                    "address": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
                    "blockHash": "0x9c51acbbe2bc5d186cba24d46ba8140d6a72805c715315daa1e71c052913010d",
                    "blockNumber": 15188712,
                },
            },
            True,
        ),
    ],
)
def test_db_insertion(mongo_connection_string, event_name, message_data, is_supported):
    """
    Testing if the data sent to the persistence module is correctly processed and saved to DB
    There are 2 cases to teste:
    - supported events (currently only PairCreated)
    - unsupported events (all except the above)
    """
    storage = StorageSystem(mongo_connection_string)
    inserted = storage.insert_event(event_name, message_data)
    document = storage.events_db[event_name].find_one({"_id": inserted})
    if not is_supported:
        assert document is None
    else:
        document.pop("_id")
        insertion_time = document.pop("t")
        assert document == message_data
        assert isinstance(insertion_time, int)
        assert get_utc_time_now() <= insertion_time
