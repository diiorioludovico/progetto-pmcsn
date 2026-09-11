from rngs import plantSeeds, selectStream, random
from rvgs import Exponential

# Parametri del sistema #
START = 0.0
STOP = 100000.0 
INFINITY = (100.0 * STOP)

MEAN_INTERRARIVAL_TIME = 1      # λ ≈ 1 job/s
MEAN_EDGE_SERVICE_TIME = 0.5    # 1/μ_e = 0.5 s
MEAN_CLOUD_SERVICE_TIME = 0.8   # 1/μ_u = 0.8 s
MEAN_C_EDGE_SERVICE_TIME = 0.1  # 1/μ_c = 0.1 s
P_C = 0.3                      # Probabilità di feedback verso il Cloud

arrivalTemp = START  

# CLASSI
class EventList:
    edge_arrival = INFINITY     # prossimo arrivo estreno di un job di classe-E al nodo edge
    edge_completion = INFINITY  # prossimo completamento di un job al nodo edge (Classe-E o Classe-C)
    cloud_completion = INFINITY # prossimo completamento di un job al nodo cloud (Classe-U)

    def min(self):
        return min(self.edge_arrival, self.edge_completion, self.cloud_completion)

class Time:
    current = -1
    next = -1
    last = -1

class SystemState:
    n_e = 0 # numero di job di classe-E dentro il nodo edge
    n_c = 0 # numero di job di classe-C dentro il nodo edge
    n_u = 0 # numero di job di classe-U dentro il nodo cloud    
    departed_jobs = 0 # numero di job completati usciti dal sistema

class Track:
    def __init__(self):
        self.node = 0.0     # area integrata nel nodo (Queue + Service)
        self.queue = 0.0    # area integrata in coda
        self.service = 0.0  # area integrata in servizio (tempo di occupazione server)

# FUNZIONI PER I GENERATORI
def get_arrival():
    global arrivalTemp
    selectStream(0) 
    arrivalTemp += Exponential(MEAN_INTERRARIVAL_TIME)
    return arrivalTemp

def get_edge_service():
    selectStream(1)
    return Exponential(MEAN_EDGE_SERVICE_TIME)

def get_cloud_service():
    selectStream(2)
    return Exponential(MEAN_CLOUD_SERVICE_TIME)

def get_c_edge_service():
    selectStream(3)
    return Exponential(MEAN_C_EDGE_SERVICE_TIME)

def get_feedback():
    selectStream(4)
    return random() < P_C

def run_simulation(seed, verbose=False, p_c=0.4, arrival_rate=1):
    # CONFIGURAZIONE PARAMETRI
    global P_C, arrivalTemp, MEAN_INTERRARIVAL_TIME

    arrivalTemp = START
    P_C = p_c
    MEAN_INTERRARIVAL_TIME = 1 / arrival_rate

    # INIZIALIZZAZIONE #
    state = SystemState()
    event_list = EventList()
    time = Time()

    # Variabili di tracciamento 
    edge_area = Track()     # Edge complessivo
    edge_e_area = Track()   # Edge Classe-E
    edge_c_area = Track()   # Edge Classe-C
    cloud_area = Track()    # Cloud (Classe-U)

    # Contatori completamenti ed arrivi
    total_arrivals = 0
    edge_e_completions = 0
    edge_c_completions = 0
    cloud_completions = 0

    plantSeeds(seed)

    time.current = START
    event_list.edge_arrival = get_arrival()
    job_in_service = None

    # LOOP PRINCIPALE
    while (event_list.edge_arrival < STOP) or (state.n_e > 0) or (state.n_c > 0) or (state.n_u > 0):
        time.next = event_list.min() 
        dt = time.next - time.current

        # RACCOLTA STATISTICHE NODO EDGE
        if (state.n_e + state.n_c > 0):                               
            edge_area.node    += dt * (state.n_e + state.n_c)
            edge_area.queue   += dt * (state.n_e + state.n_c - 1)
            edge_area.service += dt * 1.0

            if job_in_service == 'E':
                edge_e_area.node    += dt * state.n_e
                edge_e_area.queue   += dt * (state.n_e - 1)
                edge_e_area.service += dt * 1.0
                edge_c_area.node    += dt * state.n_c
                edge_c_area.queue   += dt * state.n_c

            elif job_in_service == 'C':
                edge_e_area.node    += dt * state.n_e
                edge_e_area.queue   += dt * state.n_e
                edge_c_area.node    += dt * state.n_c
                edge_c_area.queue   += dt * (state.n_c - 1)
                edge_c_area.service += dt * 1.0

        # RACCOLTA STATISTICHE NODO CLOUD
        if (state.n_u > 0):                               
            cloud_area.node    += dt * state.n_u
            cloud_area.queue   += dt * (state.n_u - 1)
            cloud_area.service += dt * 1.0

        time.current = time.next

        # GESTIONE DEGLI EVENTI

        # 1. ARRIVO ESTERNO AL NODO EDGE (Classe-E)
        if time.current == event_list.edge_arrival:
            total_arrivals += 1
            state.n_e += 1
            event_list.edge_arrival = get_arrival()
            if event_list.edge_arrival > STOP:
                event_list.edge_arrival = INFINITY
                time.last = time.current

            if (state.n_e + state.n_c) == 1:
                job_in_service = 'E'
                event_list.edge_completion = time.current + get_edge_service()

        # 2. COMPLETAMENTO NEL NODO EDGE
        elif time.current == event_list.edge_completion:
            if job_in_service == 'E':
                edge_e_completions += 1
                state.n_e -= 1
                if get_feedback():
                    state.n_u += 1
                    if state.n_u == 1:
                        event_list.cloud_completion = time.current + get_cloud_service()
                else:
                    state.departed_jobs += 1
            elif job_in_service == 'C':
                edge_c_completions += 1
                state.n_c -= 1
                state.departed_jobs += 1

            # Selezione prossimo job con Priorità Non-Preemptive (C prima di E)
            if (state.n_e + state.n_c) > 0:
                if state.n_c > 0:
                    job_in_service = 'C'
                    service_time = get_c_edge_service()
                else:
                    job_in_service = 'E'
                    service_time = get_edge_service()
                event_list.edge_completion = time.current + service_time
            else:
                event_list.edge_completion = INFINITY
                job_in_service = None

        # 3. COMPLETAMENTO NEL CLOUD (Diventa Classe-C e torna all'Edge)
        elif time.current == event_list.cloud_completion:
            cloud_completions += 1
            state.n_u -= 1
            if state.n_u == 0:
                event_list.cloud_completion = INFINITY
            else:
                event_list.cloud_completion = time.current + get_cloud_service()

            state.n_c += 1
            if (state.n_e + state.n_c) == 1:
                job_in_service = 'C'
                event_list.edge_completion = time.current + get_c_edge_service()


    # STAMPA METRICHE DI PERFORMANCE #
    T_sim = time.current
    total_edge_completions = edge_e_completions + edge_c_completions

    if verbose:
        print("\n" + "="*50)
        print("         REPORT METRICHE DI PERFORMANCE")
        print("="*50)

        print(f"\n--- INFORMAZIONI GENERALI ---")
        print(f"Tempo Totale di Simulazione ..... = {T_sim:10.2f} s")
        print(f"Job totali arrivati dal sistema . = {total_arrivals:10d}")
        print(f"Job totali completati (Sink) ... = {state.departed_jobs:10d}")

        print(f"\n--- NODO EDGE (COMPLESSIVO - PER VISITA) ---")
        print(f"Completamenti Totali Edge ....... = {total_edge_completions:10d}")
        print(f"Tempo Medio nel Nodo (T_edge) ... = {edge_area.node / total_edge_completions:10.4f} s")
        print(f"Tempo Medio in Coda (Wq_edge) ... = {edge_area.queue / total_edge_completions:10.4f} s")
        print(f"Tempo Medio di Servizio (S_edge)  = {edge_area.service / total_edge_completions:10.4f} s")
        print(f"Numero Medio di Job nel Nodo .... = {edge_area.node / T_sim:10.4f}")
        print(f"Numero Medio di Job in Coda ..... = {edge_area.queue / T_sim:10.4f}")
        print(f"Utilizzazione Server Edge (rho) . = {edge_area.service / T_sim:10.4f}")

        print(f"\n--- DETTAGLIO CLASSE-E (NODO EDGE) ---")
        print(f"Completamenti Classe-E Edge ..... = {edge_e_completions:10d}")
        print(f"Tempo Medio Nodo (T_E) .......... = {edge_e_area.node / edge_e_completions:10.4f} s")
        print(f"Tempo Medio Coda (Wq_E) ......... = {edge_e_area.queue / edge_e_completions:10.4f} s")
        print(f"Tempo Medio Servizio (S_E) ...... = {edge_e_area.service / edge_e_completions:10.4f} s")
        print(f"Numero Medio di Job E nel Nodo .. = {edge_e_area.node / T_sim:10.4f}")

        print(f"\n--- DETTAGLIO CLASSE-C (NODO EDGE) ---")
        print(f"Completamenti Classe-C Edge ..... = {edge_c_completions:10d}")
        print(f"Tempo Medio Nodo (T_C) .......... = {edge_c_area.node / edge_c_completions:10.4f} s")
        print(f"Tempo Medio Coda (Wq_C) ......... = {edge_c_area.queue / edge_c_completions:10.4f} s")
        print(f"Tempo Medio Servizio (S_C) ...... = {edge_c_area.service / edge_c_completions:10.4f} s")
        print(f"Numero Medio di Job C nel Nodo .. = {edge_c_area.node / T_sim:10.4f}")

        print(f"\n--- NODO CLOUD (CLASSE-U - PER VISITA) ---")
        print(f"Completamenti Totali Cloud ...... = {cloud_completions:10d}")
        print(f"Tempo Medio nel Nodo Cloud (T_U)  = {cloud_area.node / cloud_completions:10.4f} s")
        print(f"Tempo Medio in Coda Cloud (Wq_U)  = {cloud_area.queue / cloud_completions:10.4f} s")
        print(f"Tempo Medio di Servizio (S_U) ... = {cloud_area.service / cloud_completions:10.4f} s")
        print(f"Numero Medio di Job nel Cloud ... = {cloud_area.node / T_sim:10.4f}")
        print(f"Numero Medio di Job in Coda Cloud = {cloud_area.queue / T_sim:10.4f}")
        print(f"Utilizzazione Server Cloud (rho)  = {cloud_area.service / T_sim:10.4f}")

        print(f"\n--- METRICHE GLOBALI SISTEMA (END-TO-END) ---")
        print(f"Throughput Globale (lambda_sys) . = {state.departed_jobs / T_sim:10.4f} job/s")
        print(f"Tempo Medio Risposta Globale (T_sys) = {(edge_area.node + cloud_area.node) / state.departed_jobs:10.4f} s")
        print(f"Tempo Medio Coda Globale (Wq_sys) .. = {(edge_area.queue + cloud_area.queue) / state.departed_jobs:10.4f} s")
        print(f"Numero Medio Totale Job nel Sistema  = {(edge_area.node + cloud_area.node) / T_sim:10.4f}")
        print("="*50)

    return {
        "T_E": edge_e_area.node / edge_e_completions
    }


if __name__ == "__main__":
    # ESEMPIO DI ESECUZIONE
    run_simulation(seed=8376249, verbose=True, p_c=0.7, arrival_rate=1.38231)
