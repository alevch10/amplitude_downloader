
```
amplitude_loader/
├── main.py          # FastAPI app
├── s3_client.py     # Класс для boto3
├── amplitude.py     # Логика скачивания и обработки
├── requirements.txt # Зависимости
├── .env             # Переменные
└── README.md        # Описание
```

Настроить SSH для GitHub Actions: 
```
touch ~/.ssh/github_actions 
chmod 600 ~/.ssh/github_actions
vim ~/.ssh/github_actions 
```
В открытый файл вставить приватный ключ 

Задать использование ключа для хоста: 
```
ssh-keyscan github.com >> ~/.ssh/known_hosts
vim ~/.ssh/config
```
Добавить: 
```
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/github_actions
  IdentitiesOnly yes
```

Проверить соединение:
``` 
ssh -T git@github.com
```

Открыть порт: 
```
sudo ufw enable
sudo ufw allow OpenSSH  
sudo ufw allow 8000       
sudo ufw status
```
Настроить systemd:
```
touch /etc/systemd/system/fastapi-app.service
vim /etc/systemd/system/fastapi-app.service
```
Вставить: 
```
[Unit]
Description=FastAPI app for amplitude-downloader
After=network.target

[Service]
User=root
WorkingDirectory=/root/code/amplitude_downloader
ExecStart=/usr/bin/env poetry run uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
Environment="PATH=/root/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

[Install]
WantedBy=multi-user.target
```

```
sudo systemctl daemon-reload
sudo systemctl restart fastapi-app
```