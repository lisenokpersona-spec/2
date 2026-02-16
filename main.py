import telebot
from telebot import types
import time
import traceback
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os

# 1. Ваши токены и ID
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# Настраиваем сессию с повторными попытками и увеличенными таймаутами
session = requests.Session()
retry = Retry(
    total=3,
    read=3,
    connect=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 504)
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
session.mount('http://', adapter)
session.mount('https://', adapter)

# Создаем бота
bot = telebot.TeleBot(TELEGRAM_TOKEN, parse_mode="HTML")

# 2. Хранилища для логов сообщений
messages_log = {}
message_senders = {}
business_connection_owners = {}
active_chats = set()
business_connections = {}

# Список администраторов для рассылки
ADMIN_IDS = [1007477341]

# Состояния для рассылки
user_states = {}
broadcast_data = {}

# Конфигурация типов контента
CONTENT_TYPE_CONFIG = {
    'text': {
        'emoji': '📝',
        'name': 'Сообщение',
        'get_content': lambda msg: msg.text,
        'send_method': 'send_message',
        'has_caption': False
    },
    'photo': {
        'emoji': '🖼️',
        'name': 'Фото',
        'get_content': lambda msg: msg.photo[-1].file_id,
        'send_method': 'send_photo',
        'has_caption': True
    },
    'video': {
        'emoji': '🎥',
        'name': 'Видео',
        'get_content': lambda msg: msg.video.file_id,
        'send_method': 'send_video',
        'has_caption': True
    },
    'document': {
        'emoji': '📄',
        'name': 'Документ',
        'get_content': lambda msg: msg.document.file_id,
        'send_method': 'send_document',
        'has_caption': True
    },
    'animation': {
        'emoji': '🎬',
        'name': 'GIF/Анимация',
        'get_content': lambda msg: msg.animation.file_id,
        'send_method': 'send_animation',
        'has_caption': True
    },
    'voice': {
        'emoji': '🎤',
        'name': 'Голосовое',
        'get_content': lambda msg: msg.voice.file_id,
        'send_method': 'send_voice',
        'has_caption': False
    },
    'audio': {
        'emoji': '🎵',
        'name': 'Аудио',
        'get_content': lambda msg: msg.audio.file_id,
        'send_method': 'send_audio',
        'has_caption': False
    },
    'sticker': {
        'emoji': '🩷',
        'name': 'Стикер',
        'get_content': lambda msg: msg.sticker.file_id,
        'send_method': 'send_sticker',
        'has_caption': False
    },
    'location': {
        'emoji': '📍',
        'name': 'Локация',
        'get_content': lambda msg: f"[location] lat={msg.location.latitude}, lon={msg.location.longitude}",
        'send_method': 'send_message',
        'has_caption': False
    },
    'contact': {
        'emoji': '👤',
        'name': 'Контакт',
        'get_content': lambda msg: f"[contact] {msg.contact.first_name} {msg.contact.last_name or ''}, tel={msg.contact.phone_number}",
        'send_method': 'send_message',
        'has_caption': False
    }
}

def get_chat_title(chat: telebot.types.Chat) -> str:
    """Возвращает удобочитаемое название чата."""
    if chat.type == "private":
        full_name = ""
        if chat.first_name:
            full_name += chat.first_name
        if chat.last_name:
            full_name += f" {chat.last_name}"
        if not full_name and chat.username:
            full_name = f"@{chat.username}"
        return full_name.strip() if full_name else str(chat.id)
    else:
        return chat.title if chat.title else str(chat.id)

def get_user_info(user: telebot.types.User) -> str:
    """Возвращает информацию о пользователе."""
    user_info = ""
    if user.first_name:
        user_info += user.first_name
    if user.last_name:
        user_info += f" {user.last_name}"
    if user.username:
        user_info += f" (@{user.username})"
    return user_info.strip() if user_info else f"User_{user.id}"

def get_bot_owner_id(business_connection_id: str) -> int:
    """Определяет ID владельца бота для данного бизнес-соединения."""
    return business_connection_owners.get(business_connection_id)

def safe_send(chat_id: int, content_type: str, content, caption: str = "", **kwargs):
    """
    Универсальная функция для безопасной отправки любого типа контента.
    """
    max_retries = 3
    config = CONTENT_TYPE_CONFIG.get(content_type)
    
    if not config:
        print(f"❌ Неизвестный тип контента: {content_type}")
        return False
    
    send_method = getattr(bot, config['send_method'])
    
    for attempt in range(max_retries):
        try:
            # Формируем аргументы для отправки
            if content_type in ['text', 'location', 'contact']:
                result = send_method(chat_id, content, **kwargs)
            elif config['has_caption']:
                result = send_method(chat_id, content, caption=caption, **kwargs)
            else:
                result = send_method(chat_id, content, **kwargs)
            
            print(f"✅ {config['name']} отправлено в чат {chat_id}")
            return result
            
        except requests.exceptions.Timeout:
            print(f"⏱️ Таймаут при отправке в чат {chat_id}, попытка {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"❌ Превышено количество попыток для чата {chat_id}")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка отправки в чат {chat_id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return False

def validate_business_connection(business_connection_id: str) -> int:
    """
    Проверяет и возвращает ID владельца бизнес-соединения.
    Если соединение не найдено, пытается получить информацию из API.
    """
    if not business_connection_id:
        print("⚠️ Сообщение без бизнес-соединения")
        return None
    
    # Если уже есть в кэше
    if business_connection_id in business_connection_owners:
        return business_connection_owners[business_connection_id]
    
    # Пытаемся получить из API
    try:
        business_connection_info = bot.get_business_connection(business_connection_id)
        owner_id = business_connection_info.user.id
        business_connection_owners[business_connection_id] = owner_id
        business_connections[business_connection_id] = business_connection_info
        
        if owner_id not in active_chats:
            active_chats.add(owner_id)
        
        print(f"✅ Зарегистрирован владелец: {owner_id} для соединения {business_connection_id}")
        return owner_id
        
    except Exception as e:
        print(f"❌ Ошибка получения бизнес-соединения {business_connection_id}: {e}")
        return None

def extract_message_data(message: telebot.types.Message) -> dict:
    """
    Извлекает данные из сообщения в универсальном формате.
    """
    content_type = message.content_type
    config = CONTENT_TYPE_CONFIG.get(content_type)
    
    if not config:
        print(f"⚠️ Неизвестный тип сообщения: {content_type}")
        return None
    
    data = {
        "type": content_type,
        "chat_id": message.chat.id,
        "business_connection_id": message.business_connection_id,
        "content": config['get_content'](message)
    }
    
    # Добавляем caption если поддерживается
    if config['has_caption'] and message.caption:
        data["caption"] = message.caption
    
    return data

def format_content_display(content_type: str, content: str, caption: str = "") -> str:
    """
    Форматирует контент для отображения в уведомлениях.
    """
    config = CONTENT_TYPE_CONFIG.get(content_type)
    if not config:
        return f"[{content_type}] {content}"
    
    display = f"{config['emoji']} {config['name']}"
    
    if content_type in ['text', 'location', 'contact']:
        display = content
    elif config['has_caption'] and caption:
        display += f"\n\nПодпись: {caption}"
    
    return display

def broadcast_message(broadcast_type: str, content: str, caption: str = ""):
    """Функция рассылки сообщения всем пользователям бота."""
    success_count = 0
    fail_count = 0
    
    print(f"🔄 Начало рассылки. Тип: {broadcast_type}")
    
    for chat_id in active_chats:
        result = safe_send(chat_id, broadcast_type, content, caption)
        if result:
            success_count += 1
        else:
            fail_count += 1
        time.sleep(0.1)
    
    return success_count, fail_count

# --- Хендлер для рассылки ---
@bot.message_handler(func=lambda message: message.text == "304041GHK")
def handle_broadcast_command(message: telebot.types.Message):
    """Обрабатывает команду для открытия меню рассылки."""
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ У вас нет прав для использования этой команды.")
        return
    
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton(text=f"{config['emoji']} {config['name']}", 
                                  callback_data=f"broadcast_{ctype}")
        for ctype, config in CONTENT_TYPE_CONFIG.items()
        if ctype in ['text', 'photo', 'video', 'document', 'animation']
    ]
    
    for i in range(0, len(buttons), 2):
        keyboard.add(*buttons[i:i+2])
    
    keyboard.add(types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast"))
    
    user_states[message.chat.id] = "broadcast_menu"
    
    safe_send(
        message.chat.id,
        'text',
        f"📋 <b>Меню рассылки</b>\n\n"
        f"Выберите тип контента для рассылки:\n\n"
        f"Статистика:\n"
        f"• Активных чатов: {len(active_chats)}\n"
        f"• Владельцев бизнес-ботов: {len(business_connection_owners)}",
        reply_markup=keyboard
    )

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """Обрабатывает callback-запросы от кнопок."""
    if call.data.startswith("broadcast_"):
        broadcast_type = call.data.replace("broadcast_", "")
        user_states[call.message.chat.id] = f"waiting_broadcast_{broadcast_type}"
        
        if call.message.chat.id not in broadcast_data:
            broadcast_data[call.message.chat.id] = {}
        broadcast_data[call.message.chat.id]['type'] = broadcast_type
        
        config = CONTENT_TYPE_CONFIG.get(broadcast_type, {})
        instruction = f"{config.get('emoji', '📋')} <b>Отправьте {config.get('name', 'контент')} для рассылки:</b>"
        
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast"))
        
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=instruction,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"❌ Ошибка редактирования сообщения: {e}")
        
    elif call.data == "cancel_broadcast":
        user_states.pop(call.message.chat.id, None)
        broadcast_data.pop(call.message.chat.id, None)
        
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text="❌ Рассылка отменена.",
                reply_markup=None
            )
        except Exception as e:
            print(f"❌ Ошибка отмены рассылки: {e}")
    
    elif call.data == "confirm_broadcast":
        data = broadcast_data.get(call.message.chat.id, {})
        broadcast_type = data.get('type')
        content = data.get('content')
        caption = data.get('caption', "")
        
        if not content:
            bot.answer_callback_query(call.id, "❌ Контент для рассылки не найден")
            return
        
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text="🔄 <b>Запуск рассылки...</b>\n\nПожалуйста, подождите.",
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"❌ Ошибка обновления сообщения: {e}")
        
        success_count, fail_count = broadcast_message(broadcast_type, content, caption)
        
        user_states.pop(call.message.chat.id, None)
        broadcast_data.pop(call.message.chat.id, None)
        
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"📊 <b>Результаты рассылки:</b>\n\n"
                     f"✅ Успешно отправлено: {success_count}\n"
                     f"❌ Не удалось отправить: {fail_count}\n"
                     f"📈 Всего пользователей: {len(active_chats)}",
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"❌ Ошибка отправки результатов: {e}")

# --- Универсальный обработчик для broadcast контента ---
@bot.message_handler(content_types=['text', 'photo', 'video', 'document', 'animation'],
                    func=lambda msg: user_states.get(msg.chat.id, "").startswith("waiting_broadcast_"))
def handle_broadcast_content(message: telebot.types.Message):
    """Универсальный обработчик для всех типов контента при рассылке."""
    broadcast_type = user_states[message.chat.id].replace("waiting_broadcast_", "")
    
    data = extract_message_data(message)
    if not data:
        return
    
    if message.chat.id not in broadcast_data:
        broadcast_data[message.chat.id] = {}
    
    broadcast_data[message.chat.id]['content'] = data['content']
    broadcast_data[message.chat.id]['caption'] = data.get('caption', '')
    
    preview_text = format_content_display(broadcast_type, data['content'], data.get('caption', ''))
    show_broadcast_preview(message.chat.id, preview_text, CONTENT_TYPE_CONFIG[broadcast_type]['name'])

def show_broadcast_preview(chat_id: int, content: str, content_type: str):
    """Показывает предпросмотр рассылки и кнопки подтверждения."""
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="✅ Подтвердить рассылку", callback_data="confirm_broadcast"))
    keyboard.add(types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_broadcast"))
    
    safe_send(
        chat_id,
        'text',
        f"📝 <b>Предпросмотр рассылки:</b>\n\n"
        f"Тип: {content_type}\n"
        f"Содержимое:\n{content}\n\n"
        f"<i>Это сообщение будет отправлено {len(active_chats)} пользователям.</i>\n"
        f"Подтвердите отправку:",
        reply_markup=keyboard
    )

# --- Хендлеры для «Business Mode» ---

@bot.business_connection_handler()
def handle_business_connection(connection: telebot.types.BusinessConnection):
    """Обрабатывает подключение бизнес-аккаунта."""
    print(f"🔌 Получено бизнес-соединение: {connection.id}")
    print(f"   Владелец: {connection.user.id}")
    print(f"   Активно: {connection.is_enabled}")
    
    business_connection_owners[connection.id] = connection.user.id
    business_connections[connection.id] = connection
    
    if connection.user.id not in active_chats:
        active_chats.add(connection.user.id)
        print(f"✅ Владелец {connection.user.id} добавлен в активные чаты")

@bot.business_message_handler(content_types=[
    'text', 'photo', 'video', 'voice', 'document',
    'animation', 'audio', 'sticker', 'location', 'contact'
])
def handle_business_message(message: telebot.types.Message):
    """Обрабатывает новые бизнес-сообщения и логирует их."""
    owner_id = validate_business_connection(message.business_connection_id)
    if not owner_id:
        return
    
    data = extract_message_data(message)
    if not data:
        return
    
    # Сохраняем информацию об отправителе
    if message.from_user:
        message_senders[(message.chat.id, message.message_id)] = {
            'info': get_user_info(message.from_user),
            'user_id': message.from_user.id
        }
    
    messages_log[(message.chat.id, message.message_id)] = data
    print(f"💾 Сообщение сохранено: чат {message.chat.id}, тип {data['type']}")

@bot.edited_business_message_handler(content_types=[
    'text', 'photo', 'video', 'voice', 'document', 
    'animation', 'audio', 'sticker', 'location', 'contact'
])
def handle_edited_business_message(message: telebot.types.Message):
    """Обрабатывает отредактированные сообщения."""
    owner_id = validate_business_connection(message.business_connection_id)
    if not owner_id:
        return
    
    print(f"✏️ Обнаружено редактирование сообщения {message.message_id} в чате {message.chat.id}")
    
    old_data = messages_log.get((message.chat.id, message.message_id), {})
    new_data = extract_message_data(message)
    
    if not new_data:
        return
    
    # Обновляем лог
    messages_log[(message.chat.id, message.message_id)] = new_data
    
    # Получаем информацию об отправителе
    sender_data = message_senders.get((message.chat.id, message.message_id), {})
    sender_info = sender_data.get('info', "Неизвестный отправитель")
    sender_user_id = sender_data.get('user_id')
    
    # Если отправитель - владелец, не уведомляем
    if sender_user_id == owner_id:
        print(f"⏩ Сообщение отредактировано владельцем {owner_id}, уведомление не отправляется")
        return
    
    # Формируем уведомление
    old_content = format_content_display(old_data.get('type', 'unknown'), 
                                        old_data.get('content', '?'),
                                        old_data.get('caption', ''))
    new_content = format_content_display(new_data['type'], 
                                        new_data['content'],
                                        new_data.get('caption', ''))
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="Перейти в чат", url=f"tg://user?id={message.chat.id}"))
    
    notify_text = (
        f"✏️ <b>Сообщение отредактировано</b>\n"
        f"от: {sender_info}\n\n"
        f"<b>Было:</b> {old_content}\n\n"
        f"<b>Стало:</b> {new_content}\n\n"
        f"@{bot.get_me().username}"
    )
    
    print(f"📤 Отправка уведомления об редактировании владельцу {owner_id}")
    safe_send(owner_id, 'text', notify_text, reply_markup=keyboard)

@bot.deleted_business_messages_handler()
def handle_deleted_business_messages(deleted: telebot.types.BusinessMessagesDeleted):
    """Обрабатывает удаленные бизнес-сообщения."""
    owner_id = validate_business_connection(deleted.business_connection_id)
    if not owner_id:
        return
    
    chat_id = deleted.chat.id
    
    # Не уведомляем, если удаление в чате с владельцем
    if chat_id == owner_id:
        print(f"⏩ Сообщение удалено владельцем {owner_id}, уведомление не отправляется")
        return
    
    print(f"🔄 Обработка удаленных сообщений: чат {chat_id}, владелец {owner_id}")
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="Перейти в чат", url=f"tg://user?id={chat_id}"))
    
    for msg_id in deleted.message_ids:
        data = messages_log.pop((chat_id, msg_id), None)
        sender_data = message_senders.pop((chat_id, msg_id), {})
        
        sender_info = sender_data.get('info', "Неизвестный отправитель")
        sender_user_id = sender_data.get('user_id')
        
        # Пропускаем, если владелец удалил свое сообщение
        if sender_user_id == owner_id:
            print(f"⏩ Сообщение удалено владельцем, пропускаем")
            continue
        
        if not data:
            notify_text = (
                f"🗑️ Сообщение удалено\n"
                f"от: {sender_info}\n\n"
                f"Сообщение не сохранено (ОШИБКА: ЛОГИ)\n"
                f"📋 ID сообщения: {msg_id}\n\n"
                f"@{bot.get_me().username}"
            )
            safe_send(owner_id, 'text', notify_text, reply_markup=keyboard)
            continue
        
        content_type = data.get("type")
        content = data.get("content")
        caption = data.get("caption", "")
        
        print(f"🔄 Восстановление удаленного сообщения типа {content_type}")
        
        try:
            config = CONTENT_TYPE_CONFIG.get(content_type)
            if not config:
                continue
            
            prefix = f"@{bot.get_me().username}\n\n🗑️ <b>Удаленное {config['name']}</b>\nот {sender_info}"
            
            if content_type == 'text':
                restored_text = f"{prefix}:\n\n{content}"
                safe_send(owner_id, 'text', restored_text, reply_markup=keyboard)
            elif content_type == 'sticker':
                safe_send(owner_id, 'text', f"{prefix}\n\n", reply_markup=keyboard)
                bot.send_sticker(owner_id, content)
            elif config['has_caption']:
                full_caption = prefix
                if caption:
                    full_caption += f"\nподпись: {caption}"
                safe_send(owner_id, content_type, content, caption=full_caption, reply_markup=keyboard)
            else:
                safe_send(owner_id, content_type, content, caption=prefix, reply_markup=keyboard)
            
            print(f"✅ Уведомление о удалении отправлено владельцу {owner_id}")
            
        except Exception as e:
            error_text = (
                f"❌ <b>Ошибка при восстановлении сообщения:</b>\n"
                f"Тип: {content_type}\n"
                f"Ошибка: {str(e)}"
            )
            safe_send(owner_id, 'text', error_text, reply_markup=keyboard)

# --- Обычные команды ---
@bot.message_handler(commands=['start', 'help'])
def handle_start_help(message: telebot.types.Message):
    active_chats.add(message.chat.id)
    
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(types.InlineKeyboardButton(text="Перейти в канал", url="https://t.me/DLmgg"))
    
    safe_send(
        message.chat.id,
        'text',
        "<b>🤖 Добро пожаловать! Этот бот создан для отслеживания удаленных сообщений.</b>\n\n"
        "Функционал:\n"
        "• Моментальные уведомления об удаленных сообщениях\n"
        "(Голосовое, фото и пр.)\n"
        "• Моментальные уведомления об ОТРЕДАКТИРОВАННЫХ сообщениях\n\n"
        "<i>💡Как подключить бота - смотрите на картинку выше!</i>",
        reply_markup=keyboard
    )
    
    try:
        with open('DLM_instruction.png', 'rb') as photo:
            bot.send_photo(message.chat.id, photo, caption="Инструкция по подключению")
    except FileNotFoundError:
        print("❌ Файл с инструкцией не найден")
    except Exception as e:
        print(f"❌ Ошибка при отправке фото: {e}")

if __name__ == "__main__":
    print("🚀 Бот запущен и ждёт сообщений...")
    print(f"📊 Текущая статистика:")
    print(f"   Активных чатов: {len(active_chats)}")
    print(f"   Бизнес-соединений: {len(business_connection_owners)}")
    
    while True:
        try:
            bot_info = bot.get_me()
            print(f"✅ Бот авторизован: @{bot_info.username}")
            
            bot.polling(
                none_stop=True,
                interval=1,
                timeout=60,
                allowed_updates=[
                    "message", 
                    "callback_query", 
                    "business_connection",
                    "business_message", 
                    "edited_business_message", 
                    "deleted_business_messages"
                ]
            )
            
        except telebot.apihelper.ApiTelegramException as e:
            print(f"❌ Ошибка Telegram API: {e}")
            if "Forbidden" in str(e):
                print("⚠️ Бот заблокирован пользователем")
            print("🔄 Переподключение через 10 секунд...")
            time.sleep(10)
            
        except requests.exceptions.Timeout:
            print("⏱️ Таймаут соединения, переподключение...")
            time.sleep(5)
            
        except ConnectionError as e:
            print(f"❌ Ошибка подключения: {e}")
            print("🔄 Переподключение через 15 секунд...")
            time.sleep(15)
            
        except Exception as e:
            print(f"❌ Неизвестная ошибка: {type(e).__name__}: {e}")
            print("Трассировка ошибки:")
            traceback.print_exc()
            print("🔄 Перезапуск через 20 секунд...")
            time.sleep(20)
