# Project-Taxi
Итоговый проект №5 для курса Data Engineer на тему: Клиенты и счета (Такси)

**Цель проекта**: на основе данных поездок Taxi г. Нью-Йорк построить таблицу-отчет(далее "parquet") со следующей информацией для каждого дня:
 * процент поездок по количеству человек в машине (5 групп пассажиров)
 * Самая дорогая поездка для каждой группы пассажиров
 * Самая дешевая поездка для каждой группы пассажиров

**Доп. задача**: Провести аналитику и построить график на тему "как пройденное расстояние и количество пассажиров влияет на размер чаевых"

**Примечания**
1) Изначально планировал выполнить весь проект в IntelliJ IDEA, но написав код, не смог собрать проект в sbt, выдавало ошибки deduplicate ... (их около 100). Плюс файл parquet не сохранялся ни в каком формате.
2) В итоге создал ноутбуки на Scala, изначально выполнял там аналитику, но потом создал ноутбук еще и по Main_Object (Уже там смог сохранить parquet в формате "csv" и "parquet")
3) Код для построения parquet (включающий все группы пассажиров) отрабатывает за 3.5 - 5 минут (Смотрел в SparkUI localhost:4040)
4) Также чисто функциями Spark не строились все даты (дни), например, если был 0, то Spark выдавал просто пустую строку (не null тип, а просто пустоту), удалось решить данную проблему сложным Spark SQL запросом. Иначе parquet криво строился. Это объяснение насчет долгого времени выполнения кода.

**Стек проекта**
 * Git
 * Scala v. 2.11.8 (библиотека Vegas v 0.3.11 для анализа данных)
 * Spark v. 2.4.0
 * Docker
 * Образ JupyterLab со Spark: JupyterLab v 2.1.4 - Spark v 2.4.0 (ссылка на образ (git): https://github.com/cluster-apps-on-docker/spark-standalone-cluster-on-docker)
 * Canva для выполнения презентации

**Структура проекта**
Project-Taxi<br>
&emsp;-notebooks <br>
&emsp;&emsp;-Check.ipynb <br>
&emsp;&emsp;-Taxi_analysis.ipynb <br>
&emsp;&emsp;-Taxi_parquet.ipynb 
&emsp;-results <br>
&emsp;&emsp;-parquet.csv <br>
&emsp;&emsp;-result.parquet <br>
&emsp;-scala <br>
&emsp;&emsp;-Main_Object.scala <br>
&emsp;&emsp;-Taxi_Analysis.scala <br>
&emsp;-LICENCE <br>
&emsp;-README.md <br>
&emsp;-Project_Taxi.pdf <br>

**Комментарий по структуре проекта**
1) В папке notebooks находятся ноутбуки для удобного просмотра выполнения задач проекта:
 * Check.ipynb - код для проверки открытия сохраненного parquet в двух форматах
 * Taxi_analysis.ipynb - задание по аналитике данных
 * Taxi_parquet.ipynb - задание по выполнению основного задания
2) В папке results находится parquet в формате "csv" и "parquet"
3) В папке scala находится Scala код для решения осн. и доп. задания
4) LICENCE - лицензия, используемая в git репозитории проекта
5) README.md - детальное описание проекта
6) Project_Taxi.pdf - презентация в "pdf" формате

**Руководство по использованию проекта**
 * Данные поездок Taxi могут быть скачаны по ссылке: https://disk.yandex.ru/d/DKeoopbGH1Ttuw
 * Файлы формата "ipynb" могут быть открыты в JupyterNotebook или Google Colab. Также приложил ссылку на образ докера, в котором есть и JupyterLab, и Spark под капотом. Уже все настроено, можно удобно пользоваться.
 * Файлы формата "scala" могут быть добавлены в проект в IntelliJ IDEA. Тогда дополнительно надо добавить следующие зависимости в структуру проекта: <br>
&emsp;scalaVersion := "2.11.8" <br>
&emsp;sparkVersion = "2.3.4" <br>
&emsp;vegasVersion = "0.3.11" <br>
&emsp;libraryDependencies ++= Seq( <br>
&emsp;&emsp;"org.apache.spark" %% "spark-core" % sparkVersion, <br>
&emsp;&emsp;"org.apache.spark" %% "spark-sql" % sparkVersion, <br>
&emsp;&emsp;"org.vegas-viz" %% "vegas" % vegasVersion)