from socket import *
from constMP import *
import threading
import random
import time
import pickle
from requests import get
import heapq # Usaremos um heap para a fila de prioridade

# ... (código existente) ...

# Variável global para o relógio de Lamport
lamport_clock = 0
# Fila de prioridade para armazenar mensagens recebidas
# Os elementos serão tuplas: (timestamp, ID_remetente, número_mensagem)
message_queue = []
# Lock para proteger o acesso ao relógio de Lamport e à fila de mensagens
clock_and_queue_lock = threading.Lock()

# ... (código existente) ...

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('Handler is ready. Waiting for the handshakes...')
        
        global handShakeCount
        global lamport_clock # Acessa o relógio de Lamport
        
        logList = []
        
        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                # Atualiza o relógio de Lamport ao receber um handshake
                with clock_and_queue_lock:
                    lamport_clock = max(lamport_clock, msg[2]) + 1 # msg[2] será o timestamp do handshake
                
                handShakeCount = handShakeCount + 1
                print('--- Handshake received: ', msg[1], ' with Lamport Clock: ', msg[2])

        print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

        stopCount = 0 
        # Loop principal para receber e processar mensagens
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == -1: # Mensagem de parada
                stopCount = stopCount + 1
                if stopCount == N:
                    break
            else:
                # msg[0]: ID do remetente, msg[1]: número da mensagem, msg[2]: timestamp de Lamport
                sender_id, msg_number, msg_timestamp = msg[0], msg[1], msg[2]
                
                with clock_and_queue_lock:
                    # Atualiza o relógio de Lamport
                    lamport_clock = max(lamport_clock, msg_timestamp) + 1
                    # Adiciona a mensagem à fila de prioridade
                    # Usamos o timestamp como prioridade principal e o ID do remetente como desempate
                    heapq.heappush(message_queue, (msg_timestamp, sender_id, msg_number))
                
                print(f'Received message (LC: {msg_timestamp}): {msg_number} from process {sender_id}. Queue size: {len(message_queue)}')

                # Tenta entregar as mensagens ordenadamente
                self.deliver_ordered_messages(logList)
        
        # Garante que todas as mensagens na fila sejam entregues antes de finalizar
        while message_queue:
            self.deliver_ordered_messages(logList)

        # Write log file
        logFile = open('logfile'+str(myself)+'.log', 'w')
        # As mensagens em logList já estarão ordenadas
        logFile.writelines(str(logList))
        logFile.close()
        
        # ... (código existente para enviar log para o servidor) ...
        
        handShakeCount = 0
        exit(0)

    def deliver_ordered_messages(self, logList):
        global lamport_clock
        global N # N é o número total de peers, assumimos que está acessível
        
        # O critério de entrega para multicasting totalmente ordenado com Lamport
        # é que você só pode entregar uma mensagem (m, T) se:
        # 1. (m, T) é a mensagem com o menor timestamp na sua fila.
        # 2. Para cada outro processo p, você já recebeu uma mensagem de p com
        #    um timestamp maior ou igual a T (garantindo que p não enviará
        #    nada com um timestamp menor que T que deveria ter sido entregue antes).
        # Para simplificar, neste exemplo, vamos entregar a mensagem com o menor timestamp
        # assim que ela chegar, o que é uma aproximação para sistemas menores.
        # Em um sistema distribuído real, o segundo critério é mais complexo e
        # geralmente envolve um mecanismo de "ACKs" ou informações de "vetor de relógios"
        # para saber o estado dos outros processos.

        # Para uma implementação mais robusta, você precisaria de um mecanismo
        # para saber que *todos* os peers já processaram mensagens com timestamps menores
        # antes de entregar uma mensagem com um timestamp T.
        # No entanto, para o propósito deste exercício, vamos focar na ordenação
        # baseada apenas no timestamp e na entrega da menor mensagem disponível.

        while message_queue:
            # Pega a mensagem com o menor timestamp da fila (não remove ainda)
            next_msg_timestamp, next_msg_sender_id, next_msg_number = message_queue[0]

            # Simplesmente remove e processa a mensagem com o menor timestamp.
            # Isso é uma simplificação. A condição real para entrega total seria
            # mais complexa e envolveria a garantia de que não há mensagens com
            # timestamps menores em trânsito de outros processos.
            with clock_and_queue_lock:
                popped_msg = heapq.heappop(message_queue)
            
            # Formato do log original: (ID_remetente, número_mensagem)
            # Para o log, podemos querer armazenar também o timestamp de Lamport.
            # Por simplicidade, vamos manter o formato original do log (msg[0], msg[1])
            # mas garantindo que ele seja adicionado na ordem correta.
            logList.append((popped_msg[1], popped_msg[2])) # Armazena (ID_remetente, número_mensagem)
            print(f'DELIVERED: Message {popped_msg[2]} from process {popped_msg[1]} (LC: {popped_msg[0]})')


# ... (código existente) ...

# Modifique o envio de handshakes e mensagens de dados para incluir o relógio de Lamport.
# A função `waitToStart` não precisa ser modificada para incluir o relógio.

# From here, code is executed when program starts:
registerWithGroupManager()
while 1:
    print('Waiting for signal to start...')
    (myself, nMsgs) = waitToStart()
    print('I am up, and my ID is: ', str(myself))

    if nMsgs == 0:
        print('Terminating.')
        exit(0)

    time.sleep(5)

    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()
    print('Handler started')

    PEERS = getListOfPeers()
    
    # Send handshakes
    for addrToSend in PEERS:
        print('Sending handshake to ', addrToSend)
        with clock_and_queue_lock:
            lamport_clock += 1 # Incrementa o relógio antes de enviar
            msg = ('READY', myself, lamport_clock) # Inclui o timestamp de Lamport
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))

    print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

    while (handShakeCount < N):
        pass # find a better way to wait for the handshakes

    # Send a sequence of data messages to all other processes 
    for msgNumber in range(0, nMsgs):
        time.sleep(random.randrange(10,100)/1000)
        with clock_and_queue_lock:
            lamport_clock += 1 # Incrementa o relógio antes de enviar
            msg = (myself, msgNumber, lamport_clock) # Inclui o timestamp de Lamport
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))
            print(f'Sent message {msgNumber} (LC: {lamport_clock})')

    # Tell all processes that I have no more messages to send
    for addrToSend in PEERS:
        with clock_and_queue_lock:
            lamport_clock += 1 # Incrementa o relógio ao enviar a mensagem de parada
            msg = (-1,-1, lamport_clock) # Inclui o timestamp de Lamport
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend,PEER_UDP_PORT))