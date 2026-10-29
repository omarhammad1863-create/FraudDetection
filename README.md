# SentinelPay — حفظ النتائج، Velocity، وIdempotency

التطبيق بيقيّم **محاولة دفع** ويحفظ نتيجة الفحص. لسه مفيش خصم فلوس أو اتصال ببنك.
وثيقة المعمارية الكبيرة بتوصف مراحل مستقبلية؛ الملف ده بيشرح الموجود فعليًا.

```text
POST /pay + Idempotency-Key
    -> PostgreSQL: احجز المفتاح، أو رجّع النتيجة القديمة
    -> Redis: سجّل المحاولة واحسب محاولات المستخدم في آخر 5 دقائق
    -> gRPC: ابعت المبلغ والعدد لخدمة الاحتيال
    -> PostgreSQL: احفظ النتيجة والرد مع بعض
    -> HTTP: رجّع النتيجة للمستخدم
```

## التشغيل من PowerShell

المطلوب Python 3.14 وDocker Desktop شغال على Linux containers.
الكونتينرات للـPostgreSQL وRedis، والخدمتين Python بيشتغلوا محليًا عشان يسهل تتبّعهم في الـIDE.

من فولدر المشروع، اعمل البيئة الافتراضية لو مش موجودة:

```powershell
python -m venv .venv
```

وبعدين:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/fraud.proto
docker compose up -d --wait
```

أمر التوليد لازم يتعاد بعد تعديل العقد. شغّل النسختين المحدثتين من الخدمتين؛ العميل القديم اللي مش بيبعت العدد هيتقابل بخطأ تحقق.

في Terminal 1:

```powershell
.\.venv\Scripts\python.exe fraud_service.py
```

في Terminal 2:

```powershell
.\.venv\Scripts\python.exe -m uvicorn payment_service:app --host 127.0.0.1 --port 8000
```

خدمة الدفع بتطبّق ملفات `migrations/*.sql` عند البداية، مرة واحدة لكل ملف، وتسجّلها في `schema_migrations`.
لو PostgreSQL مش متاح، التطبيق مش هيكمل بدء التشغيل. لو Redis أو gRPC غير متاحين، الطلبات الجديدة هترجع خطأ، لكن النتائج المحفوظة يمكن إعادتها من PostgreSQL.

افتح [Swagger](http://127.0.0.1:8000/docs) لتجربة `POST /pay` و`GET /payments/{payment_id}`.

## جرّب بنفسك

في Terminal ثالث، جهّز مستخدم جديد ومفتاح جديد:

```powershell
$demoCustomer = 'demo_' + [guid]::NewGuid().ToString('N')
$demoKey = [guid]::NewGuid().ToString('N')
$demoBody = @{ customer_id = $demoCustomer; amount = 150 } | ConvertTo-Json
$demoHeaders = @{ 'Idempotency-Key' = $demoKey }

$result = Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/pay -ContentType 'application/json' -Headers $demoHeaders -Body $demoBody
$result

# نفس المفتاح ونفس البيانات: نفس النتيجة ونفس id، من غير عدّ جديد.
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/pay -ContentType 'application/json' -Headers $demoHeaders -Body $demoBody

# اقرأ النتيجة المحفوظة.
Invoke-RestMethod -Uri "http://127.0.0.1:8000/payments/$($result.id)"

# ثلاث محاولات جديدة لنفس المستخدم: العدّ يصبح 2 ثم 3 ثم 4.
# المحاولة الرابعة تتمنع حتى لو المبلغ صغير.
1..3 | ForEach-Object {
    $nextHeaders = @{ 'Idempotency-Key' = [guid]::NewGuid().ToString('N') }
    Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/pay -ContentType 'application/json' -Headers $nextHeaders -Body $demoBody
}

# نفس المفتاح الأصلي مع مبلغ مختلف: HTTP 422 (متوقع).
$changedBody = @{ customer_id = $demoCustomer; amount = 151 } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/pay -ContentType 'application/json' -Headers $demoHeaders -Body $changedBody
```

مثال شكل الرد (الـid والتوقيت بيتغيّروا):

```json
{
  "id": "4785a117-fbf7-444c-9a63-f185661897be",
  "customer_id": "customer_1",
  "amount_cents": 15000,
  "currency": "EGP",
  "decision": "ALLOW",
  "tx_count_5m": 1,
  "created_at": "2026-10-29T10:00:00+00:00"
}
```

## الاتفاق الحالي

- `customer_id` مطلوب: من 1 إلى 64 حرفًا من الحروف الإنجليزية والأرقام و`_` و`-`، وحالة الحروف تفرق.
- `amount` بالجنيه المصري، موجب، بحد أقصى 12 رقمًا ومنزلتين عشريتين. بيتحوّل لعدد صحيح بالقروش قبل التخزين والإرسال.
- `Idempotency-Key` مطلوب: من 1 إلى 128 حرفًا من الحروف الإنجليزية والأرقام و`._:-`. استخدم UUID جديدًا لكل محاولة جديدة، واحتفظ بنفس المفتاح عند إعادة **نفس** الطلب.
- الحقول الإضافية غير مقبولة، لتجنّب تجاهل بيانات العميل بدون ما يعرف.
- أكثر من 10,000 جنيه **أو** المحاولة الرابعة وما بعدها في آخر 5 دقائق: `BLOCK`. غير كده: `ALLOW`.
- العدّ يشمل المحاولة الحالية، والمحاولات اللي نتيجتها `BLOCK`، والمحاولات اللي وصلت Redis وبعدها فشل gRPC أو حفظ النتيجة.
- النافذة متحركة: `(الوقت الحالي - 5 دقائق، الوقت الحالي]`، مش عدّاد بيتصفّر كل خمس دقائق ثابتة.
- `ALLOW` و`BLOCK` الاتنين نتائج فحص ناجحة بـHTTP 200، وبيتحفظوا في PostgreSQL.

| الحالة | الرد |
|---|---|
| نفس المفتاح ونفس البيانات بعد اكتمال الطلب | نفس body مع `Idempotent-Replay: true` |
| نفس المفتاح مع تغيير المبلغ أو المستخدم | 422 |
| نفس الطلب لسه بيتعالج | 409؛ أعد المحاولة بنفس المفتاح |
| بيانات غير صالحة أو مفتاح مفقود | 422 |
| Redis أو PostgreSQL أو اتصال gRPC غير متاح | 503 |
| مهلة gRPC (ثانية واحدة) انتهت | 504 |
| خطأ gRPC آخر أو قرار غير معروف | 502 |
| رقم سجل غير موجود عند القراءة | 404 |

## كل ملف موجود ليه؟

| الملف | المسؤولية |
|---|---|
| `payment_service.py` | استقبال الطلب، تجهيز الموارد، تنسيق الخطوات، وتحويل الأخطاء لردود HTTP |
| `storage.py` | تطبيق تغييرات الجداول، حجز مفاتيح التكرار، وحفظ النتيجة والرد |
| `migrations/001_payment_evaluations.sql` | تعريف الجداول والقيود، قابل للقراءة بعيدًا عن كود HTTP |
| `velocity.py` | سكريبت Redis اللي يسجّل ويحسب المحاولات كعملية ذرية واحدة |
| `fraud_service.py` | صاحب قرار الاحتيال: قاعدة المبلغ وقاعدة سرعة المحاولات |
| `proto/fraud.proto` | عقد الرسائل؛ أضفنا `tx_count_5m = 2` وحافظنا على رقم حقل المبلغ |
| `fraud_pb2.py` و`fraud_pb2_grpc.py` | ملفات مولّدة؛ لا تتعدل يدويًا |
| `settings.py` | عناوين الخدمات، قابلة للتغيير بمتغيرات البيئة |
| `compose.yaml` | PostgreSQL وRedis مع فحص جاهزية وتخزين دائم |
| `tests/test_integration.py` | اختبارات فعلية باستخدام PostgreSQL وRedis وgRPC |

## ليه منع التكرار في PostgreSQL؟

قبل حساب السرعة بنحفظ المفتاح وبصمة البيانات و`attempt_id` ثابت. الحجز ده بيتم في معاملة قصيرة مستقلة، فلو العملية فشلت بعده بنستخدم نفس المعرف عند الإعادة.

بعدها بنقفل صف المفتاح بـ`FOR UPDATE NOWAIT`. نسخة تانية من التطبيق مش هتقدر تعالج نفس المفتاح في نفس اللحظة. القفل بيتحرر عند إنهاء المعاملة، أو لو الاتصال اتقطع؛ مفيش علامة `processing` ممكن تفضل عالقة للأبد بعد موت البرنامج.

النتيجة والرد المحفوظ بيتكتبوا في **نفس معاملة PostgreSQL**. لو الكتابة فشلت، الاتنين بيتراجعوا. لو الكتابة نجحت والرد ما وصلش للمستخدم، إعادة نفس المفتاح ترجع النتيجة المحفوظة.

Redis بيستخدم `attempt_id` لمنع العدّ المكرر، وبيحتفظ بعدد المحاولة الأصلي لمدة 5 دقائق. لو محاولة **لم تكتمل** اتعادت بعد انتهاء المدة، بنعمل فحص جديد في النافذة الحالية. النتيجة المكتملة محفوظة في PostgreSQL وبتتعاد مهما عدّى وقت، من غير الرجوع لـRedis أو gRPC.

## حدود المرحلة دي

- مفيش خصم مالي ولا بنوك، ومفيش Kafka أو WebSockets لسه.
- نسخة تعليمية محلية بدون تسجيل دخول أو تعدد merchants. `customer_id` جاي من العميل، ومفاتيح التكرار عامة على التطبيق كله. قبل فتح API لناس مختلفة لازم هوية موثوقة وعزل وصلاحيات للقراءة.
- بنحتفظ بقفل ومعاملة PostgreSQL أثناء Redis وgRPC، بمهل قصيرة. ده تبسيط مناسب للحجم الحالي؛ مع التوسع نراجع استخدام الاتصالات وتصميم تنفيذ المهام.
- PostgreSQL وRedis مش معاملة واحدة مشتركة. تصميم الإعادة يعالج الفشل المعتاد، لكنه مش ضمان حفظ عدّ Redis لو بيانات Redis نفسها ضاعت.
- Redis عنده volume وAOF كل ثانية، لكن انهيار مفاجئ ممكن يفقد آخر جزء غير مكتوب. مفيش إعادة بناء لعداداته من PostgreSQL في المرحلة دي.
- حجوزات المفاتيح ونتائجها مفيش لها حذف تلقائي حاليًا. أخطاء الخدمات مؤقتة ولا تتحفظ كرد نهائي، لكن المفتاح يفضل مرتبطًا بنفس البيانات.
- بيانات دخول Compose للتطوير المحلي فقط، والبورتات مربوطة بـ`127.0.0.1`.

## الإعدادات وفحص البيانات

| متغير البيئة | القيمة الافتراضية |
|---|---|
| `DATABASE_URL` | `postgresql://sentinel:sentinel_local@127.0.0.1:55433/sentinelpay` |
| `REDIS_URL` | `redis://127.0.0.1:56379/0` |
| `FRAUD_TARGET` | `127.0.0.1:50051` (وجهة خدمة الدفع) |
| `FRAUD_BIND` | `127.0.0.1:50051` (استماع خدمة الاحتيال) |

لتغيير قيمة، استخدم مثلًا `$env:REDIS_URL = 'redis://127.0.0.1:56379/0'` قبل تشغيل Python.
متغيرا Compose `POSTGRES_PORT` و`REDIS_PORT` يغيّران البورتات المنشورة؛ لو غيّرتهم لازم تغيّر عناوين التطبيق المقابلة كمان.

```powershell
docker compose ps
docker compose exec postgres psql -U sentinel -d sentinelpay -c "SELECT * FROM payment_evaluations ORDER BY created_at DESC LIMIT 20;"
docker compose exec postgres psql -U sentinel -d sentinelpay -c "SELECT idempotency_key, attempt_id, response_body IS NOT NULL AS completed FROM idempotency_requests;"
docker compose exec redis redis-cli --scan --pattern 'sentinel:velocity:*'
```

للإيقاف: Ctrl+C للخدمتين، ثم `docker compose stop`.
`docker compose down` بيزيل الكونتينرات مع الاحتفاظ بالـvolumes. إضافة `-v` بتحذف البيانات، فمتستخدمهاش إلا لو قاصد تبدأ من الصفر.

## الاختبارات

خلي الكونتينرات شغّالة. الاختبارات بتشغّل سيرفرات gRPC مؤقتة تلقائيًا، واختبار منها بيشغّل الخدمتين كعمليتين منفصلتين. مش لازم تشغّل خدمات Python بنفسك للاختبارات.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

كل اختبار بيعمل schema مؤقتة ومستخدمين بمعرفات فريدة وينظّف بياناته فقط. مفيش `FLUSHDB` أو تفريغ لجداول التطبيق. استخدم قاعدة تطوير تسمح بإنشاء schemas. لو عملية الاختبار اتقتلت فجأة قد تترك schema باسم `sentinel_test_...`.

التغطية تشمل حدود المبلغ، المحاولة الرابعة، عزل المستخدمين، العدّ المتزامن، تكرار الطلب عبر نسختين من التطبيق، تغيير بيانات المفتاح، انتهاء النافذة، أخطاء Redis وgRPC، فشل حفظ الرد، انقطاع اتصال PostgreSQL أثناء المعاملة، استعادة محاولة بعد انقطاع صاحب القفل، وعودة نفس النتيجة بعد إعادة تشغيل خدمة الدفع.

مراجع الآليات المستخدمة: [Redis Lua atomic execution](https://redis.io/docs/latest/develop/programmability/eval-intro/)، [PostgreSQL locks](https://www.postgresql.org/docs/current/explicit-locking.html)، [asyncpg transactions](https://magicstack.github.io/asyncpg/current/usage.html).
