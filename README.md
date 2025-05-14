1. 建立環境
    
    ```powershell
    docker-compose down
    docker-compose build
    docker-compose up -d
    ```
    
    這時候就可以打開 http://localhost:4200/dashboard
    
2. 進入容器，確認版本 `prefect version`
    
    ```powershell
    docker exec -it prefect-etl bash
    ```
    
    - 若要測試 flow
        
        ```powershell
        python flows/flow1.py
        ```
        
    - 簡易部屬測試
        
        ```powershell
        python deployments.py
        ```

3. 建立 Work Pool Agent  
先建立一個本地 work-pool（假設叫 default-process-pool）：

    ```powershell
    prefect work-pool create default-process-pool -t process
    ```
    
4. 部署 flow（建立 Deployment，指定它要屬於哪個 pool）  
成功後會在 Prefect UI 指定的 Work Pool Deployments 頁面看到。

    ```powershell
    prefect deploy --all
    ```
    
5. 啟動  Work Pool  
一定要啟動才能跑部署的 flow。
    
    ```powershell
    prefect worker start --pool default-process-pool
    ```
"# CDP-ETL-V2" 
