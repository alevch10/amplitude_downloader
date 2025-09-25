
```
amplitude_loader/
├── main.py          # FastAPI app
├── s3_client.py     # Класс для boto3
├── amplitude.py     # Логика скачивания и обработки
├── requirements.txt # Зависимости
├── .env             # Переменные
└── README.md        # Описание
```

Залить SSH для GitHub Actions: 
touch ~/.ssh/github_actions 
- выставить приватный ключ 
chmod 600 ~/.ssh/github_actions
vim ~/.ssh/config
Добавить: 
```
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/github_actions
  IdentitiesOnly yes
```
чек соединения: 
ssh -T git@github.com

git ls-remote github.com/alevch10/amplitude_downloader - проверить доступ
ssh-keyscan github.com >> ~/.ssh/known_hosts

Фаервол: 
```
sudo ufw enable
sudo ufw allow OpenSSH  
sudo ufw allow 8000       
sudo ufw status
```