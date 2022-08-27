<details>
  <summary>Tfswjfs ?</summary>
  
  J'ai appliqué le chiffrement de césar avec une clé de 1 -> évite de référencer la page.
</details>

**Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses
volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?**

Si l'on a disons 1 fichier de 1TO dans chacun des trois dossiers, on les partitionne pour les séparer sur plusieurs machines
Une fois qu'on a des millions de fichiers de petite volumétrie (< 1 MO), on utilise soit Hadoop ou pyspark/Spark soit le cloud pour faire les traitements(selon les contraintes)

-> Pour faire cette tâche, on peut soit passer par le cloud soit le faire en local avec du pyspark/Spark

---
## Partie 2: SQL

**Chiffre d’affaires jour par jour, du 1er janvier 2019 au 31 décembre 2019 trié par ordre croissant:**

SELECT date, SUM(prod_price * prod_qty) as ventes <br>
FROM TRANSACTION <br>
WHERE date between "01/01/2019" and "31/12/2019" <br>
GROUP BY date ASC


**Chiffre d’affaires par client pour les ventes meubles et déco, du 1er janvier 2020 au 31 décembre 2020:**

WITH ventes_meuble as ( <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; SELECT client_id, SUM(prod_price * prod_qty) as ventes_meuble <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; FROM TRANSACTION t INNER JOIN PRODUCT_NOMENCLATURE p on t.prod_id = p.product_id <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; WHERE product_type = "MEUBLE" <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; AND date between "01/01/2020" and "31/12/2020" <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; GROUP BY client_id <br>
), <br>
ventes_deco as ( <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; SELECT client_id, SUM(prod_price * prod_qty) as ventes_deco <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; FROM TRANSACTION t inner join PRODUCT_NOMENCLATURE p on t.prod_id = p.product_id <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; WHERE product_type = "DECO" <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; AND date between "01/01/2020" and "31/12/2020" <br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; GROUP BY client_id <br>
) <br> <br>
SELECT m.client_id, ventes_meuble, ventes_deco <br>
FROM ventes_meuble m FULL JOIN ventes_deco d on m.client_id = d.client_id
