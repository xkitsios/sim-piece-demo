import pandas as pd

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.NumpyTablet import NumpyTablet


def connect():
    ip = "127.0.0.1"
    port_ = "6667"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_)
    return session


# def parse_file(file):
#     df = pd.read_csv(file, header=None)
#     data_types = [TSDataType.FLOAT]
#     np_values = [df[1].to_numpy(dtype=TSDataType.FLOAT.np_dtype())]
#     np_timestamps = df[0].to_numpy(dtype=TSDataType.FLOAT.np_dtype())
#     measurements = ["value"]
#
#     return data_types, np_values, np_timestamps, measurements


# def chimp(session, measurements, data_types, np_values, np_timestamps):
#     if session.check_time_series_exists("root.demo.chimp"):
#         session.delete_time_series(["root.demo.chimp"])
#     session.create_time_series(
#         "root.demo.chimp", TSDataType.FLOAT, TSEncoding.CHIMP, Compressor.UNCOMPRESSED
#     )
#     np_tablet = NumpyTablet(
#         "root.demo.chimp.ts1", measurements, data_types, np_values, np_timestamps
#     )
#     session.insert_tablet(np_tablet)


# def sim_piece(session, measurements, data_types, np_values, np_timestamps):
#     if session.check_time_series_exists("root.demo.sim_piece"):
#         session.delete_time_series(["root.demo.sim_piece"])
#     session.create_time_series(
#         "root.demo.sim_piece", TSDataType.FLOAT, TSEncoding.SIM_PIECE, Compressor.UNCOMPRESSED
#     )
#     np_tablet = NumpyTablet(
#         "root.demo.sim_piece.ts1", measurements, data_types, np_values, np_timestamps
#     )
#     session.insert_tablet(np_tablet)
#
#     session.insert_tablet(np_tablet)


def main():
    session = connect()
    session.open(False)

    df = pd.read_csv("dataset/WindDirection.csv.gz", header=None)

    session.set_storage_group("root.demo")

    if session.check_time_series_exists("root.demo.sim_piece"):
        session.delete_time_series(["root.demo.sim_piece"])
    session.create_time_series("root.demo.sim_piece", TSDataType.FLOAT, TSEncoding.SIM_PIECE, Compressor.UNCOMPRESSED)

    if session.check_time_series_exists("root.demo.chimp"):
        session.delete_time_series(["root.demo.chimp"])
    session.create_time_series("root.demo.chimp", TSDataType.FLOAT, TSEncoding.SIM_PIECE, Compressor.UNCOMPRESSED)

    session.insert_records_of_one_device(
        "root.demo.sim_piece",
        list(df[0]),
        [["t" + str(t) for t in df[0]]],
        [[TSDataType.FLOAT for _ in range(df.shape[0])]],
        [list(df[1])]
    )

    session.insert_records_of_one_device(
        "root.demo.chimp",
        list(df[0]),
        [["t" + str(t) for t in df[0]]],
        [[TSDataType.FLOAT for _ in range(df.shape[0])]],
        [list(df[1])]
    )

    # session.delete_storage_group("root.demo")

    session.close()


if __name__ == "__main__":
    main()
