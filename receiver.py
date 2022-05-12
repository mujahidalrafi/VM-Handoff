import gzip

import sender
import socket
import pickle
from sender import TransmittableChunk

def main():

    vm = sender.VM()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((sender.HOST, sender.PORT))
        s.listen()
        while True:
            conn, addr = s.accept()
            with conn:
                print(f"Connected by {addr}")
                allData = []
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    # conn.sendall(data)
                    allData.append(data)
                compressedChanges = b"".join(allData)
                vm.ReceiveAndApplyChanges(compressedChanges)



                # while True:
                #     data = conn.recv(int(1e10))
                #     if not data:
                #         break
                #     # conn.sendall(data)
                #     modifications = pickle.loads(data)
                #
                #     print(f"Received {len(modifications)} items.")
                #     for index, value in modifications.items():
                #         print(f"[{index}]: {value}")




if __name__ == '__main__':
    main()