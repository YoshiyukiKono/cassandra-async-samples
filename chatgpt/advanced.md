`CompletableFuture.allOf`を使って非同期処理をループ内で実現する方法では、複数の`CompletableFuture`を収集し、全ての処理が完了するのを待つ設計を採用します。このアプローチを用いることで、クエリ数が増えてもコードを柔軟に拡張できます。

以下に、Cassandraの`executeAsync`を使ったループ処理の例を示します。

---

### サンプルコード

```java
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSetFuture;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class CassandraAsyncWithAllOf {
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build()) {

            // テーブル準備（初期設定）
            session.execute("CREATE KEYSPACE IF NOT EXISTS test_keyspace " +
                    "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
            session.execute("CREATE TABLE IF NOT EXISTS test_keyspace.users (" +
                    "id UUID PRIMARY KEY, name TEXT, age INT);");

            // PreparedStatement
            PreparedStatement preparedStatement = session.prepare(
                    "INSERT INTO test_keyspace.users (id, name, age) VALUES (uuid(), ?, ?);");

            // 非同期クエリを実行するCompletableFutureのリストを保持
            List<CompletableFuture<Void>> futures = new ArrayList<>();

            // 1000件のデータを挿入
            for (int i = 0; i < 1000; i++) {
                int finalI = i;

                // CompletableFutureを作成し、リストに追加
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        ResultSetFuture resultSetFuture = session.executeAsync(
                                preparedStatement.bind("User" + finalI, 20 + finalI));
                        resultSetFuture.get(); // 非同期クエリの結果を取得（ここで待機）
                        System.out.println("Inserted: User" + finalI);
                    } catch (Exception e) {
                        System.err.println("Error inserting User" + finalI + ": " + e.getMessage());
                    }
                });

                futures.add(future);
            }

            // 全ての非同期処理が完了するまで待つ
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0]));

            allFutures.thenRun(() -> {
                System.out.println("All inserts completed successfully!");
            }).exceptionally(throwable -> {
                System.err.println("Error during inserts: " + throwable.getMessage());
                return null;
            });

            // 完了を待つ
            allFutures.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

---

### コードの解説

1. **クエリを非同期で実行する`CompletableFuture`をリストに追加**:
   - 各クエリを`CompletableFuture.runAsync()`で非同期に実行します。
   - その処理を`futures`リストに追加して管理します。

2. **`CompletableFuture.allOf`で全ての非同期処理を監視**:
   - `CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))`を用いて、すべての非同期処理が完了するのを待ちます。

3. **`thenRun`で完了後の処理を定義**:
   - 全ての非同期処理が成功した場合に実行される処理を`thenRun`で定義します。
   - エラー発生時のハンドリングは`exceptionally`を使用します。

4. **`allFutures.join()`で同期的に待機**:
   - 最後に、`allFutures.join()`を呼び出すことで、メインスレッドが全タスクの完了を待機します。

---

### 実行結果例

成功時:
```
Inserted: User0
Inserted: User1
...
Inserted: User999
All inserts completed successfully!
```

エラー発生時（例: 一部クエリが失敗）:
```
Inserted: User0
Error inserting User1: TimeoutException
...
Error during inserts: java.util.concurrent.CompletionException: ...
```

---

### 応用: バックプレッシャー制御を組み込む

バックプレッシャーをさらに適切に制御したい場合、以下の手法を併用します。

1. **同時実行数の制限**:
   `ExecutorService`を使用して非同期タスクのスレッド数を制御。

```java
ExecutorService executor = Executors.newFixedThreadPool(10);
CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
    // 非同期処理
}, executor);
```

2. **タスクキューの導入**:
   同時実行数を制限しつつ、処理待ちのタスクをキューに格納。

3. **リトライ処理**:
   クエリの実行が失敗した場合に再試行するロジックを組み込みます。

---

この方法を採用することで、Apache Cassandraで大量データを効率的に非同期でインサートでき、全体の完了を安全に待つことができます。
