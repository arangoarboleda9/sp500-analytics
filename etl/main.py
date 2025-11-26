import time

print("========== SP500 ETL CONTAINER ==========")

print("Arrancando proceso ETL de prueba...")

for i in range(5):
    print(f"Procesando paso {i + 1}/5...")
    time.sleep(1)

print("ETL de prueba finalizado correctamente ✅")
