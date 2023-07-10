# Breve descripcion del proceso

## Proposito del proceso
El procedimiento consiste en obtener y almacenar las tasas de cambio de una divisa especificada (por defecto sera en dolares) a otros tipos de divisas (peso mexicano, euro, libra, etc).
El proceso obtiene los datos de una API proveida por la siguiente pagina: [https://www.frankfurter.app/](https://www.frankfurter.app/)
El objetivo es ir almacenando un historico diario de las tasas de cambio habidas a travez de los dias y realizar un analisis de como ha ido evolucionando el valor de la tasa de la divisa base indicada en comparacion con todas las demas divisas.

Se tiene tambien funcionalidad de validar un valor minimo alcanzado de cierta divisa especificada (por defecto esta configurada la divisa MXN) para asi indicar por medio de un correo electronico que se ha rebasado dicho valor minimo especificado. Un ejemplo, por defecto el proceso tiene la indicacion de enviar un correo cuando el valor del dolar ha superado los 18.0 pesos mexicanos, indicando en el correo dicho evento. En caso contrario, no se realizara el envio de dicha notificacion.

Es importante aclarar que las tasas solo se actualizan en dias habiles, por lo que existe una validacion que indica que si el dia en el que se ejecuta el proceso no es un dia habil, el flujo terminara en dicha validacion.

### Parametros configurables
El archivo keys/config.json contiene las configuraciones principales. Las configuraciones son las siguientes:

#### Configuraciones de BD
- BD_HOST: La URL del servidor de base de datos
- BD_PORT: Puerto de la BD
- BD_NAME: Nombre de la BD
- BD_USER: Usuario de la BD
- BD_PASS: Contraseña de la BD
- DATA_TABLE: Contiene el nombre y las columnas de la tabla principal

#### Configuraciones de la API de tasas de cambio
- URL_BASE: URL principal de la API
- FROM_CURRENCY: Divisa de la cual se obtendra el tipo de cambio en comparacion con las otras divisas
- CURRENCY_PREFERRED: Divisa de la cual sera validado su valor con base al minimo especificado
- MINIMUM_RATE_CURRENCY: Valor minimo de la tasa que al ser rebasado el proceso enviara una  notificacion por correo

#### Configuracion del correo a usar
- EMAIL_FROM_USER: Correo de salida de las notificaciones
- EMAIL_TO: Correo de destino de las notificaciones

### Archivos con datos sensibles
En la carpeta keys existen 2 archivos: key_email y pass_email. pass_email contiene la contraseña encriptada del email de envio de notificaciones y key_email contiene la clave de desencriptacion de la contraseña del email.
