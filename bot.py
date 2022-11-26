from bot_extension import *
from bot_extension import start_analytics_autofaq
import cron


def main():
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler('start', start))
    dispatcher.add_handler(CommandHandler('menu', new_menu))

    # Создание инцидентов
    dispatcher.add_handler(SkufCreateINC.get_handler())

    # Меню настроек бота
    dispatcher.add_handler(SettingsMenu.get_handler())

    # Меню изменения параметров пользователей
    dispatcher.add_handler(EditUsersMenu.get_handler())

    # Парсер просрочек
    dispatcher.add_handler(AdminMenu.get_handler())

    # Поисковик вики
    dispatcher.add_handler(WikiMenu.get_handler())

    # Добавление сетевого инцидента NTTM
    dispatcher.add_handler(NttmMenu.get_handler_add_si())

    # Привязка к сетевому инциденту NTTM
    dispatcher.add_handler(NttmMenu.get_handler_link_ki())

    callback_handler = CallbackQueryHandler(general_callback_handler)
    dispatcher.add_handler(callback_handler)

    if database.Settings.get_working_mode() == 'prod1':
        def error_handler(update, context):
            error_text = "Ошибка при обработке запроса!"
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=error_text)
        dispatcher.add_error_handler(error_handler)

    threading.Thread(target=cron.start, args=(updater,), daemon=True).start()
    threading.Thread(target=start_analytics_autofaq, args=(updater,), daemon=True).start()
    logger.info(f"Бот запущен в режиме {database.Settings.get_working_mode()}")
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
