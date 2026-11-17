# Socket Tester

Flutter製のTCP/UDP通信評価ツールの初期実装です。

## 実装済み
- TCP Client / Server
- UDP Client / Server
- HEX / ASCII送信
- TX/RXログ（時刻、送信元/宛先、HEX、ASCII）
- 周期送信・回数指定（0は無制限）
- 複数セッション
- CSVログ保存（アプリのDocumentsフォルダ）

## 起動方法

```bash
flutter pub get
flutter run -d windows
```

Android:
```bash
flutter run -d <android-device-id>
```

iOS（macOS + Xcodeが必要）:
```bash
flutter run -d <ios-device-id>
```

## 注意事項
1. TCPはストリーム通信です。ログの1行はDartの受信イベント単位であり、相手側のsend()単位とは一致しません。
2. TCP Serverからの送信は、現バージョンでは最後に接続したクライアント宛てです。個別クライアント選択は次の改善項目です。
3. UDP Serverからの返信は、最後に受信した送信元宛てです。
4. 周期送信はOSスケジューリングの影響を受けます。1msなどの厳密なリアルタイム周期は保証しません。実測値はログの時刻から評価してください。
5. CSVは選択中セッションのログを保存します。保存先は `getApplicationDocumentsDirectory()` です。
6. ローカルネットワーク通信にはOS側の許可・ファイアウォール設定が必要な場合があります。
7. Androidのリリースビルドでは `android/app/src/main/AndroidManifest.xml` に次を追加してください:
   `<uses-permission android:name="android.permission.INTERNET" />`
8. iOSでは `ios/Runner/Info.plist` に `NSLocalNetworkUsageDescription` を設定してください。例:
   `<key>NSLocalNetworkUsageDescription</key><string>ローカルネットワーク上の機器と通信するために使用します。</string>`

## 次の開発候補
- TCP Serverの接続クライアント一覧と送信先選択
- UDP送信先の手動指定・複数宛先
- 受信ログの一時停止、検索、フィルタ
- CSV保存先をユーザーが選択できる機能
- 送信周期の実測統計（平均・最大・最小・標準偏差）
- 接続設定の保存・読込
- IPv6対応
