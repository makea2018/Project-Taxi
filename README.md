# Project-Taxi
Итоговый проект №5 для курса Data Engineer на тему: Клиенты и счета (Такси)

**Цель проекта**: на основе данных поездок Taxi г. Нью-Йорк построить таблицу-отчет(далее "parquet") со следующей информацией для каждого дня:
 * процент поездок по количеству человек в машине (5 групп пассажиров)
 * Самая дорогая поездка для каждой группы пассажиров
 * Самая дешевая поездка для каждой группы пассажиров

**Доп. задача**: Провести аналитику и построить график на тему "как пройденное расстояние и количество пассажиров влияет на размер чаевых"

**Стек проекта**
 * Git
 * Scala v. 2.11.8 (библиотека Vegas v 0.3.11 для анализа данных)
 * Spark v. 2.4.0
 * Docker
 * Образ JupyterLab со Spark: JupyterLab v 2.1.4 - Spark v 2.4.0 (ссылка на образ (git): https://github.com/cluster-apps-on-docker/spark-standalone-cluster-on-docker)
 * Canva для выполнения презентации
 * Draw.io

**Структура проекта** <br>
Project-Taxi<br>
&emsp;-notebooks <br>
&emsp;&emsp;-Check.ipynb <br>
&emsp;&emsp;-Taxi_analysis.ipynb <br>
&emsp;&emsp;-Taxi_parquet.ipynb <br>
&emsp;-results <br>
&emsp;&emsp;-graph 1.png <br>
&emsp;&emsp;-graph 2.png <br>
&emsp;&emsp;-graph 3.png <br>
&emsp;&emsp;-parquet.csv <br>
&emsp;&emsp;-result.parquet <br>
&emsp;-scala <br>
&emsp;&emsp;-Main_Object.scala <br>
&emsp;&emsp;-Taxi_Analysis.scala <br>
&emsp;-LICENCE <br>
&emsp;-README.md <br>
&emsp;-Project_Taxi.pdf <br>
&emsp;-Project-schema.svg <br>

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

**Аналитика: Графики** <br>
*График 1: Зависимость размера чаевых от кол-ва пассажиров*
![graph 1](https://user-images.githubusercontent.com/62721453/209213440-4d94b18c-4d91-4e5f-9ebe-0d7d4fabd94f.png)

*График 2: Зависимость размера чаевых от кол-ва пассажиров*
![graph 3](https://user-images.githubusercontent.com/62721453/209213497-67d0028e-a5ea-461e-963f-31933fb9397e.png)

**Выводы:** <br>
*Из графика 1 зависимости размера чаевых от кол-ва пассажиров следует, что:*

 * Самые большие чаевые давала первая группа пассажиров (1 человек)
 * Чаще всего чаевые давала первая группа пассажиров, реже всего 3 группа и 4 группа (именно где 4 пассажира)
 * В среднем можно предположить, что первая и вторая группа пассажиров самые благоприятные в плане чаевых

*Из графика 2 зависимости размера чаевых от пройденного расстояния следует, что:*
 * Не учитывая выброс (чаевые ~ 255 при расстоянии 0), то наибольшие чаевые получали при расстоянии 16-19 миль
 * Минимальные чаевые при расстоянии 2-4 мили
 * В итоге можно предположить, что лучше брать заказ на расстояние 7-10 миль или 15-20 миль, если нужны хорошие чаевые
