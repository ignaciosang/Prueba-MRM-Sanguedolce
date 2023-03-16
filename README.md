# Prueba-MRM-Sanguedolce
El arbitraje está pensado basicamente desde la paridad Ft/St. Para una misma maturity, si la tasa impicita 
del BID de un instrumento es superior a la tasa implicita dada por el ASK de otro instrumento, esto signficia que la tasa 
colocadora es mayor a la tasa tomadora, es decir, existe un spread el cual podemos arbitrar. En teoría, dado el 
spread BID-ASK, la situación debería ser la inversa, es decir, tasa tomadora < tasa colocadora. 

El programa define una Clase denominada Futuros_Bot. La clase hereda de una clase de mayor jeraquía atributos que le permiten
realizar la conexión a la API de MATBA ROFEX mediante el módulo pyRofex
Es importante aclarar que utilice conexión mediante API request y no WebSocket. Esto se debe a que las APIs fueron actualizadas
en diciembre de 2022, pero el package no, por lo que la conexión via WebSocket cuenta con algunos problemas. Si bien permite suscribir
a Market Data en tiempo real, los handlers para los mensajes no permiten modificaciones al mensaje en si mismo. Esto incluye el storage del
mensaje, lo que hace imposible manipular la entrada en tiempo real de información. Una potencial alternativa e utilizar un pakcage como std 
que permita storear todos los prints que se realizan. 

Existen dos métodos principales para este objeto. El primero es Main, que permite calcular el arbitraje para un request de inofrmación  en ese momento. 
El segundo método, Real_Time, invoca al método Main de modo iterativo y continuo, permitiendo al usuario interrumpir la función mediante input. 
Esta fue la alternativa que se implementó ante los inconvenientes en el message habdling via web socket. 
