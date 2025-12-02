# 🤖 Diegol – Bot de Telegram para automatizar casino online

Este proyecto es un bot de **Telegram** que automatiza tareas de un casino online usando:

- 🧠 Telegram (Telethon) para interactuar con los jugadores
- 🌐 Selenium para operar en la plataforma web (BlueDay / ClubUno)
- 🗄️ MySQL para guardar usuarios, cuentas y movimientos
- 📧 IMAP/Gmail para leer correos de acreditaciones (Claro Pay) y registrar ingresos

El objetivo es reducir el trabajo manual de los cajeros y operadores:
crear usuarios, cargar fichas, retirar fichas, cambiar contraseñas y registrar pagos de forma automática.

---

## ⚙️ Funcionalidades principales

- **Registro de usuarios**  
  - Pide nombre al jugador por Telegram  
  - Genera un nombre de usuario único  
  - Crea el usuario en la plataforma usando Selenium  
  - Guarda los datos en MySQL

- **Cargar fichas**  
  - Asigna una cuenta bancaria/Claro Pay de forma rotativa  
  - Lee correos de esa cuenta (Gmail IMAP)  
  - Detecta acreditaciones y las guarda en `movimientos`  
  - Carga fichas al usuario en la web con Selenium

- **Retirar fichas**  
  - El usuario indica el monto a retirar y su CBU  
  - El bot descuenta las fichas en la web  
  - Envía un aviso a un chat de administración en Telegram para que el operador haga el pago

- **Gestión de cuentas Claro Pay** (solo administrador)  
  - Agregar cuentas (alias, CBU, mail, password, titular)  
  - Listar cuentas  
  - Borrar cuentas  
  - Iniciar hilos de lectura de mails por cada cuenta activa

- **Modo mantenimiento**  
  - El operador puede poner el bot en modo “mantenimiento”  
  - Los jugadores reciben un mensaje avisando que el bot está temporalmente inactivo

---

## 🧰 Tecnologías usadas

- **Lenguaje:** Python 3
- **Telegram:** [Telethon](https://github.com/LonamiWebs/Telethon)
- **Automatización web:** Selenium + ChromeDriver
- **Base de datos:** MySQL
- **Correo:** IMAP (Gmail)
- **Otros:** threading, asyncio, colas (`queue.Queue`)

---

## 🗄️ Estructura general del código

- Manejo de usuarios y estados en un diccionario `usuarios` + tabla `usuarios` en MySQL
- Lectores de correo en **hilos independientes**, uno por cuenta Claro Pay
- Funciones utilitarias para:
  - Conexión robusta a MySQL (`mysql_connect_safe`)
  - Conexión robusta a IMAP (`imap_connect_safe`)
  - Normalización de montos y textos
- Funciones específicas de Selenium para operar en la plataforma:
  - `iniciar_sesion_blueday`
  - `crear_usuario_en_blueday`
  - `cargar_fichas_en_blueday`
  - `retirar_fichas_en_blueday`
  - `cambiar_contrasena_blueday`
  - `desbloquear_usuario_en_blueday`

---

## 🔐 Configuración (variables de entorno)

> **Importante:** El código está preparado para leer credenciales desde variables de entorno.  
> No subas tus claves reales a GitHub.

Variables esperadas (ejemplo en `.env.example`):

```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=tu_hash
TELEGRAM_PHONE=+5400000000

BLUEDAY_USER=usuario
BLUEDAY_PASS=contraseña

DB_HOST=localhost
DB_USER=root
DB_PASSWORD=tu_password
DB_NAME=claro_pay
