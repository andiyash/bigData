# Лабораторная №4
## Яшакин Андрей, группа 6132

- В Windows запускаем сервер двойным кликом по скрипту zkServer.cmd в папке ./bin/

![image](https://github.com/andiyash/bigData/assets/145579445/bc5028b8-cf66-4b83-9038-7a7f505254bf)


**Взаимодействие с ZooKeeper через командный интерфейс CLI**



- Для вывода команд используем help:

![image](https://github.com/andiyash/bigData/assets/145579445/c036fd4a-0817-4b76-a3e9-39956d6261e2)


- Находясь в консоли CLI введите команду ls / ( 2 узла так как выполнял эти действия повторно)

![image](https://github.com/andiyash/bigData/assets/145579445/ad0de3bd-a755-489b-8fd7-39b7f19dfead)




- Следующие команды возвращают данные и метаданные узла: get /mynode stat /mynode

![image](https://github.com/andiyash/bigData/assets/145579445/7175df92-b98b-43cf-8937-a10b97811d3c)


- Изменение данных узла на "second_version" 

![image](https://github.com/andiyash/bigData/assets/145579445/a4bc1ead-32cb-4266-9140-4b6b050988d8)


- Создание двух нумерованных (sequential) узла в качестве дочерних mynode

![image](https://github.com/andiyash/bigData/assets/145579445/d8a8410a-0b0e-4dcf-b25d-0920ad790e1a)


- Внутри CLI сессии, создаем узел mygroup


- Открываем две новых CLI консоли и в каждой создаем по дочернему узлу в mygroup 


- Проверяем в исходной консоли, что grue и bleen являются членами группы mygroup

![image](https://github.com/andiyash/bigData/assets/145579445/824ffe97-591b-4da9-ae25-686561686b63)

-  Выбираем консоль grue и обращаемся к информации узла bleen.

![image](https://github.com/andiyash/bigData/assets/145579445/88b3c10f-81a2-4376-bbe1-a4ca16b46386)

-  Теперь эмулируем аварийное отключение любого клиента. Нажмем сочетание клавиш Ctrl + D в одной из консолей, создавшей эфимерный узел. Проверим, что соответствующий узел пропал из mygroup. Изменение списка дочерних узлов может произойти не сразу — от 2 до 20 tickTime, значение которого можно посмотреть в zoo.cfg.
![image](https://github.com/andiyash/bigData/assets/145579445/a7301b2a-645a-4e0a-98a6-de1dd3de8b49)


-  Удаляем узел .

![image](https://github.com/andiyash/bigData/assets/145579445/06e84686-a3ae-47db-af6e-14d33d9f51de)





### Philosophers

Результат:
```
Philosopher 0 is going to eat
Philosopher 4 is going to eat
Philosopher 1 is going to eat
Philosopher 2 is going to eat
Philosopher 3 is going to eat
Philosopher 2 takes left fork
Philosopher 2 takes right fork
Philosopher 5 takes left fork
Philosopher 5 takes right fork
Philosopher 4 takes left fork
Philosopher 5 puts right fork
Philosopher 5 puts left fork 
Philosopher 4 takes right fork
Philosopher 5 is thinking
Philosopher 1 takes left fork
Philosopher 2 puts right fork
Philosopher 2 puts left fork 
Philosopher 1 takes right fork
Philosopher 3 takes left fork
Philosopher 2 is thinking
Philosopher 4 puts right fork
Philosopher 1 puts right fork
Philosopher 1 puts left fork 
Philosopher 4 puts left fork 
Philosopher 3 takes right fork
Philosopher 1 is thinking
Philosopher 4 is thinking
Philosopher 4 is going to eat
Philosopher 5 takes left fork
Philosopher 5 takes right fork
Philosopher 1 is going to eat
Philosopher 2 takes left fork
Philosopher 5 puts right fork
Philosopher 5 puts left fork 
Philosopher 1 takes left fork
Philosopher 5 is thinking
Philosopher 3 is going to eat
Philosopher 3 puts right fork
Philosopher 3 puts left fork
Philosopher 2 takes right fork
Philosopher 3 is thinking
Philosopher 4 takes left fork
Philosopher 4 takes right fork
Philosopher 2 puts right fork
Philosopher 2 puts left fork
Philosopher 1 takes right fork
Philosopher 2 is thinking
Philosopher 2 is going to eat
Philosopher 3 takes left fork
Philosopher 1 puts right fork
Philosopher 1 puts left fork
Philosopher 1 is thinking
Philosopher 4 puts right fork
Philosopher 4 puts left fork
Philosopher 3 takes right fork
Philosopher 4 is thinking
Philosopher 3 puts right fork
Philosopher 3 puts left fork
Philosopher 3 is thinking
```
## Двуфазный коммит протокол для high-available регистра

Координатор информирует исполнителей (Cleint) о начале транзакции. Затем координатор подписывается на изменения транзакционного узла (устанавливает WATCH на /app/tx). Каждый исполнитель формирует временный узел /app/tx/node_i, содержащий решение о выполнении (commit) или отмене (abort). После этого исполнитель подписывается на события своего узла, ожидая решения от координатора во второй фазе. В этой фазе координатор принимает окончательное решение (основываясь на большинстве голосов) о commit/abort, которое происходит после ожидания таймаута или создания всех временных узлов исполнителей с решением com. Затем координатор изменяет значения временных узлов для каждого исполнителя на commit/abort. Исполнители в зависимости от принятого решения либо применяют, либо отменяют транзакцию, после чего обновляют значение узла до committed.
