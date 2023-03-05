import pandas as pd

from iotdb.Session import Session
from iotdb.template.InternalNode import InternalNode
from iotdb.template.MeasurementNode import MeasurementNode
from iotdb.template.Template import Template
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.NumpyTablet import NumpyTablet


def connect():
    ip = "127.0.0.1"
    port_ = "6667"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_)
    return session


def parse_file(file):
    df = pd.read_csv(file, header=None)
    data_types = [TSDataType.FLOAT]
    np_values = [df[1].to_numpy(dtype=TSDataType.FLOAT.np_dtype())]
    np_timestamps = df[0].to_numpy(dtype=TSDataType.FLOAT.np_dtype())

    return data_types, np_values, np_timestamps


def add_timeseries(session, node, measurements, data_types, np_values, np_timestamps):
    np_tablet = NumpyTablet(
        "root.demo." + node, measurements, data_types, np_values, np_timestamps
    )
    session.insert_tablet(np_tablet)


def main():
    session = connect()
    session.open(False)

    data_types, np_values, np_timestamps = parse_file("dataset/WindDirection.csv.gz")

    demo_template = Template(name='demo_template')

    demo_node = InternalNode(name="demo", share_time=False)
    sim_piece_node = InternalNode(name="sim_piece", share_time=True)
    chimp_node = InternalNode(name="chimp", share_time=True)
    demo_node.add_child(sim_piece_node)
    demo_node.add_child(chimp_node)

    sim_piece_measurement_node = MeasurementNode("wind_sim_piece", TSDataType.FLOAT, TSEncoding.SIM_PIECE, Compressor.UNCOMPRESSED)
    sim_piece_node.add_child(sim_piece_measurement_node)

    chimp_measurement_node = MeasurementNode("wind_chimp", TSDataType.FLOAT, TSEncoding.CHIMP, Compressor.UNCOMPRESSED)
    chimp_node.add_child(chimp_measurement_node)

    demo_template.add_template(demo_node)
    demo_template.add_template(sim_piece_node)
    demo_template.add_template(chimp_node)
    demo_template.add_template(sim_piece_measurement_node)
    demo_template.add_template(chimp_measurement_node)

    session.create_schema_template(demo_template)
    session.set_storage_group("root.demo")
    session.set_schema_template('demo_template', 'root.demo')

    add_timeseries(session, "sim_piece", ["wind_sim_piece"], data_types, np_values, np_timestamps)
    add_timeseries(session, "chimp", ["wind_chimp"], data_types, np_values, np_timestamps)

    # session.delete_storage_group("root.demo")

    session.close()


if __name__ == "__main__":
    main()
