 #Breve descripcion del proceso

## Proposito del proceso

El procedimiento consiste en obtener y almacenar las tasas de cambio de una divisa especificada (por defecto sera en dolares) a otros tipos de divisas (peso mexicano, euro, libra, etc).
El proceso obtiene los datos de una API proveida por la siguiente pagina: [https://www.frankfurter.app/](https://www.frankfurter.app/)
El objetivo es ir almacenando un historico diario de las tasas de cambio habidas a travez de los dias y realizar un analisis de como ha ido evolucionando el valor de la tasa de la divisa base indicada en comparacion con todas las demas divisas.

Se tiene tambien funcionalidad de validar un valor minimo alcanzado de cierta divisa especificada (por defecto esta configurada la divisa MXN) para asi indicar por medio de un correo electronico que se ha rebasado dicho valor minimo especificado. Un ejemplo, por defecto el proceso tiene la indicacion de enviar un correo cuando el valor del dolar ha superado los 18.0 pesos mexicanos, indicando en el correo dicho evento. En caso contrario, no se realizara el envio de dicha notificacion.

Es importante aclarar que las tasas solo se actualizan en dias habiles, por lo que existe una validacion que indica que si el dia en el que se ejecuta el proceso no es un dia habil, el flujo terminara en dicha validacion.