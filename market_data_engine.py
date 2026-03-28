نعم، هذا الملف market_data_engine.py هو طبقة بيانات السوق الأساسية، وأريدك تعدله ليطابق منطق المشروع النهائي.

المطلوب:

1) هذا الملف يكون مسؤول عن تحديد ما الذي نأخذه من السوق فقط، وليس سحب كل السوق الخام.
2) لا أريد كل market feed، أريد بيانات مختصرة ومفلترة فقط.
3) اجعل المدخلات كالتالي:
   - QQQ فقط كأصل رئيسي
   - الأسهم القيادية المؤثرة على QQQ فقط
   - خيارات QQQ فقط
4) الهدف هو إرسال payload مختصر وواضح للبوت، وليس بيانات ضخمة.

التعديلات المطلوبة:

A) الأسهم:
- راقب QQQ والأسهم القيادية فقط
- ابنِ candles بإطار 30 ثانية
- لكل رمز أخرج:
  symbol, timeframe=30s, open, high, low, close, vwap, volume, timestamp

B) القياديات:
- استخدم فقط:
  AAPL, MSFT, NVDA, AMZN, GOOGL, META, AVGO, TSLA
- أرسل نفس بيانات 30 ثانية

C) الخيارات:
- أضف طبقة خيارات QQQ
- لا ترسل كل options chain
- فلتر فقط:
  - QQQ only
  - 0DTE و 1DTE فقط
  - exclude illiquid contracts
  - exclude contracts with missing bid/ask
  - keep only top candidate CALLs and PUTs
  - prefer near-the-money strikes

لكل عقد مرشح أرسل:
- contract_symbol
- side
- strike
- expiry
- dte
- bid
- ask
- mid
- volume
- open_interest
- delta if available
- iv if available

D) الإرسال:
- أرسل snapshot كل 30 ثانية، وليس كل 10 ثواني
- لا ترسل إلا البيانات المختصرة الجاهزة للقرار

شكل payload النهائي المطلوب:
{
  "timestamp": "...",
  "timeframe": "30s",
  "underlying": {...},
  "leaders": [...],
  "options_candidates": [...]
}

E) مهم:
- لا تغير منطق المشروع
- هذا الملف فقط طبقة market data preparation
- الهدف تقليل الضوضاء وتقليل استهلاك Claude وتقليل حجم البيانات
- أي معلومة لا تخدم القرار لا ترسلها

بعد التعديل:
1) اشرح لي بالضبط ما الذي أصبح يُسحب من السوق
2) اشرح لي ما الذي يتم استبعاده
3) اعطني مثال payload حقيقي نهائي
4) وضح هل بنيت 30s candles داخليًا أم اعتمدت مصدر جاهز
