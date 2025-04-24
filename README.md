1. 建立環境
    
    ```powershell
    docker-compose down
    docker-compose build
    docker-compose up -d
    ```
    
    這時候就可以打開 http://localhost:4200/dashboard
    
2. 進入容器，確認版本 `prefect version`
    
    ```powershell
    docker exec -it prefect-server bash
    ```
    
    - 若要測試 flow
        
        ```powershell
        python flows/flow1.py
        ```
        
    - 簡易部屬測試
        
        ```powershell
        python deployments.py
        ```
