import random
import threading
import time
import socket
import pickle
import gzip
import sys
import xdelta3

HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = 65432  # Port to listen on (non-privileged ports are > 1023)

# memoryBlockCount = 100
# maxModificationWindow = 10
# startHandOffAfterS = 5

memoryBlockCount = 1e7 # 10 Mega
maxModificationWindow = 1000
startHandOffAfterS = 15

avgBandwidthMbPS = 5
lockForMemory = threading.Lock()


class TransmittableChunk:
    def __init__(self, index=-1, realValue=None, encodedValue=None):
        self.index = index
        self.content = realValue
        self.encodedContent = False

        if encodedValue is not None and sys.getsizeof(realValue) > sys.getsizeof(encodedValue):
            self.encodedContent = True
            self.content = encodedValue

    def __eq__(self, other):
        if isinstance(other, TransmittableChunk):
            return self.index == other.index and self.encodedContent == other.encodedContent and \
                   self.content == other.content

        return False


class VM:

    def __init__(self):
        self.baseMemoryImage = [i.to_bytes(20, 'big') for i in range(int(memoryBlockCount))]
        self.memory = self.baseMemoryImage.copy()
        self.modificationsPending = False
        self.modifiedIndices = set()
        self.handOffComplete = False
        self.handOffStarted = False
        self.suspendApp = False

    def PrintMemory(self):
        return
        for i, memoryBlockContent in enumerate(self.memory):
            print(f"{i}: {memoryBlockContent}")

    # TODO: Calculate downtime if modifications dict is in use by SendChanges.
    def ModifyMemoryRandomly(self):
        if self.suspendApp:
            return

        with lockForMemory:
            modificationWindow = random.randint(1, maxModificationWindow)
            startingIndex = random.randint(0, memoryBlockCount - modificationWindow)
            for i in range(startingIndex, startingIndex + modificationWindow):
                newValue = random.randint(0, 2000).to_bytes(20, 'big')
                self.memory[i] = newValue
                self.modificationsPending = True
                self.modifiedIndices.add(i)

            print(f"Modified {modificationWindow} items")

    def GetTransmittableChunks(self):
        transmittableChunks = []
        # TODO: Update downtime.
        with lockForMemory:
            for i, memoryBlockContent in enumerate(self.memory):
                if memoryBlockContent != self.baseMemoryImage[i]:
                    try:
                        deltaEncoding = xdelta3.encode(self.baseMemoryImage[i], memoryBlockContent)
                    except xdelta3.NoDeltaFound:
                        deltaEncoding = None
                    finally:
                        transmittableChunk = TransmittableChunk(i, memoryBlockContent, deltaEncoding)

                    transmittableChunks.append(transmittableChunk)
                    # TODO: Reset self.baseMemoryImage when handoff is complete
                    self.baseMemoryImage[i] = memoryBlockContent

            if len(transmittableChunks) < 10:
                self.suspendApp = True

            self.modificationsPending = False

        return transmittableChunks

    # TODO: Calculate difference here instead of maintaining modifications
    def SendChanges(self):
        transmittableChunks = self.GetTransmittableChunks()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))

            data = [self.handOffComplete, transmittableChunks]
            serializedData = pickle.dumps(data)
            serializedData = gzip.compress(serializedData)

            timeBefore = time.time()
            numBytesSent = s.send(serializedData)
            timeToSend = time.time() - timeBefore
            artificialDelay = ((numBytesSent*8) / (1e6*avgBandwidthMbPS)) - timeToSend
            if artificialDelay > 0:
                print(f"Delaying {artificialDelay} second(s).")
                time.sleep(artificialDelay)

        print(f"Sent {len(transmittableChunks)} items: {numBytesSent} Bytes.")
        return numBytesSent, len(transmittableChunks)*sys.getsizeof(self.baseMemoryImage[0])

    def HandOff(self):
        print("Handoff Started.")
        timeStart = time.time()
        sentBytesTotal = 0
        originalBytesTotal = 0
        while self.modificationsPending:
            sentBytes, originalBytes = self.SendChanges()
            sentBytesTotal += sentBytes
            originalBytesTotal += originalBytes
            # time.sleep(5)

        # One final message to signal termination.
        self.handOffComplete = True
        sentBytes, originalBytes = self.SendChanges()
        sentBytesTotal += sentBytes
        originalBytesTotal += originalBytes

        self.PrintMemory()
        print(f"Handoff complete. Sent {sentBytesTotal/1e6} MB. Took {time.time() - timeStart} seconds. Original "
              f"Changes: {originalBytesTotal/1e6} MB. original Memory size: {sys.getsizeof(self.memory)/1e6} MB.")

    def ReceiveAndApplyChanges(self, compressedChanges):
        serializedChanges = gzip.decompress(compressedChanges)
        data = pickle.loads(serializedChanges)
        modifications = data[1]

        for modification in modifications:
            index = modification.index
            value = modification.content
            if modification.encodedContent:
                value = xdelta3.decode(self.baseMemoryImage[index], value)
            self.memory[index] = value

        if data[0]:
            print("COMPLETE.")
            self.PrintMemory()


def SimulateApplicationRun(vm: VM):

    startTime = time.time()
    while not vm.handOffComplete:
        timeToSleepMS = random.randint(1, 100)
        time.sleep(timeToSleepMS/1000)
        vm.ModifyMemoryRandomly()

        # Trigger HandOff
        if not vm.handOffStarted and time.time() - startTime > startHandOffAfterS:
            vm.handOffStarted = True


if __name__ == '__main__':
    vm = VM()
    app = threading.Thread(target=SimulateApplicationRun, args=(vm,), daemon=True)
    app.start()

    while not vm.handOffComplete:
        time.sleep(2)
        if vm.handOffStarted:
            vm.HandOff()

    print("End")
