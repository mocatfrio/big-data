# Recommendation Engine with Flask REST API



## Struktur Sistem

### Server.py
Server.py adalah file Python untuk menjalankan CherryPy webserver, dan memanggil App.py

### App.py
App.py adalah file Python penghubung server dengan engine, dan juga tempat routing dari URI request

### Engine.py
Engine.py adalah file Python yang menjadi "mesin" dari sistem ini. Engine.py melakukan fungsi seperti menentukan rekomendasi user, mendapatkan history user, dan lain lain



## URL yang dapat diakses

### http://<Server_IP>:5432/<user_id>/ratings/top/<count> 
  method = [GET]
  Untuk menampilkan sejumlah <count> rekomendasi film ke user <user_id>
  ![Contoh Gambar 1](./img/ratingstop.png)
  
### http://<Server_IP>:5432/movies/<movie_id>/recommend/<count> 
  method = [GET]
  Untuk menampilkan film <movie_id> terbaik direkomendasikan ke sejumlah <count> user
  ![Contoh Gambar 2](./img/recommendmovietouser.png)
  
### http://<Server_IP>:5432/<user_id>/ratings/<movie_id> 
  method = [GET]
  Untuk melakukan prediksi user <user_id> memberi rating X terhadap film <movie_id>
  ![Contoh Gambar 3](./img/predictusergiverating.png)
  
### http://<Server_IP>:5432/<user_id>/giverating 
  method = [POST]
  Untuk submit <user_id> memberikan rating untuk film X. Masukkan "movieId" dan "ratingGiven" pada body request
  ![Contoh Gambar 4](./img/usergiveratingpost.png)
  
### http://<Server_IP>:5432/<user_id>/history 
  method = [GET]
  Untuk melihat riwayat pemberian rating oleh user <user_id>
  ![Contoh Gambar 5](./img/ratinghistory.png)
  
