<details>
  <summary>Tfswjfs ?</summary>
  
  J'ai appliqué le chiffrement de césar avec une clé de 1 -> évite de référencer la page.
</details>

**Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses
volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ?**

La pipeline de données fonctionne dès lors qu'il y au moins 1 fichier dans les 3 dossiers.
Ainsi on peut partager les fichiers au sein de plusieurs machines puis faire un merge des outputs obtenues

-> Pour faire cette tâche on peut soit passer par le cloud soit le faire en local avec du pyspark/Spark



