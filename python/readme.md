# TP Coordinación 



## Modelo de Comunicación

El sistema utiliza un enfoque basado en mensajería, el mismo sigue un protocolo definido que incluye:

* Datos de negocio (fruta, cantidad)
* Identificadores de flujo
* Señales de control (EOF)

---

## Concurrencia 

Debido a las limitaciones del Global Interpreter Lock en Python, el uso de múltiples hilos no permite aprovechar completamente múltiples núcleos de CPU en tareas CPU-bound, ya que solo un hilo ejecuta bytecode a la vez dentro de un proceso.

Por este motivo, se adopta un enfoque basado en multiprocessing, desplegando múltiples procesos workers independientes (donde sea detallado en el docker compose).

---

## Coordinación

Se implementa un mecanismo de token ring para coordinar eventos globales dentro del sistema distribuido.
El token ring se utiliza exclusivamente para coordinar el cierre ordenado de los nodos y propagar señales de finalización global
El token no interviene en el procesamiento de datos, y los workers procesan mensajes en paralelo sin depender del token.

---

## Limitaciones del Token Ring

Si bien el token ring es útil para coordinación, puede introducir latencia si se utiliza en exceso.
Ademas es sensible a la caída de nodos en el anillo, en estos casos se implemento un contador de rebotes, en caso que el mensaje de EOF quede dando vueltas por la cola de Rabbit una cantidad de veces, el mismo avisa que hubo un problema y pasa al aggregator.
Por este motivo, su uso se limita a eventos de control y no al flujo principal de datos.

---

## Consideraciones

* La tolerancia a fallos del token ring es limitada
* El rendimiento depende del balanceo de carga entre workers
* Se está considerando que el volumen de datos será acotado.
* Se considera que ante una caida antes de un flush, puede haber pérdida de resultados parciales, pero el sistema seguirá funcionando.
