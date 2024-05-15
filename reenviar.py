import pandas as pd
import time
import paho.mqtt.client as mqtt
import os

# Configurações MQTT
MQTT_BROKER = "46.17.108.131"
MQTT_PORT = 1883
MQTT_BASE_TOPIC = "/TEF/unittest001/attrs/"

# Mapeamento de variáveis para seus sufixos
variable_suffix_map = {
    "carbon": "co",
    "pressure_middle": "p2",
    "pressure_bottom": "p1",
    "temperature_ext": "te",
    "temperature_int": "ti",
    "distance": "d"
}

# Arquivo de estado
STATE_FILE = 'state.txt'

# Função para carregar o índice da última linha enviada
def load_last_index():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

# Função para salvar o índice da última linha enviada
def save_last_index(index):
    with open(STATE_FILE, 'w') as f:
        f.write(str(index))

# Função para enviar dados via MQTT
def send_mqtt_data(client, variable, value):
    topic = MQTT_BASE_TOPIC + variable_suffix_map[variable]
    client.publish(topic, str(value))
    print(f"Enviado {variable} ({value}) para o tópico {topic}")

# Função principal
def main():
    # Ler o arquivo CSV
    df = pd.read_csv('dados.csv', delimiter=';')

    # Configurar o cliente MQTT
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)

    # Carregar o índice da última linha enviada
    last_index = load_last_index()

    # Enviar dados a cada 5 minutos
    for index in range(last_index, len(df)):
        row = df.iloc[index]
        for variable in variable_suffix_map.keys():
            send_mqtt_data(client, variable, row[variable])
        save_last_index(index + 1)  # Salvar o índice da próxima linha
        time.sleep(300)  # Espera 5 minutos (300 segundos)

if __name__ == "__main__":
    main()