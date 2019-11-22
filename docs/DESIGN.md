## Design

We demonstrate our key designs as follows:

### 一 视图设计
赛题中规定的查询为，对给定的marketsegment、q_orderdate、q_shipdate、q_topn，取出该marketsegment下满足orderdate < q_orderdate, shipdate > q_shipdate的同orderkey的所有lineitem的expenditure总和，并取其中expenditure总和最大的q_topn条，输出其orderkey、orderdate和expenditure总和。

基于赛题含义，我们可以将customer表和orders表通过customerkey进行join操作，将orders表和lineitem表通过orderkey进行join操作，得到一张联合的lineitem大表，每个表项包含marketsegment、customerkey、orderkey、orderdate、shipdate、expenditure。
在实际的计算中，我们并不生成这张联合的lineitem大表，而是建立一个物化视图：
- 由于查询和输出中不包含customerkey的信息，因此我们在建立和存储物化视图时省略customerkey信息；
- 由于单个查询会选中某个特定的marketsegment，因此我们以marketsegment属性进行第一层分桶；
- 由于单个查询会选中某一段orderdate，因此我们以orderdate属性进行第二层分桶。

综上，根据赛题要求，我们可以根据输入建立起一个三表联立的，以marketsegment、orderdate为键值分桶，并存储orderkey、shipdate和expenditure等属性的物化视图，然后对该物化视图进行查询。

### 二 剪枝策略
#### 数据特性分析
我们观察到，所提供的数据中，orderdate和shipdate之间具有特定的关系：
1. 根据orderdate的语义（下单时间）和shipdate的语义（订单发货时间），对于物化视图中的每个lineitem，应该满足shipdate >= orderdate（因为只有先下单了才能够进行发货）。
对dbgen文档的解读以及对所提供输入数据的profiling结果也支持了这一性质。
结合orderdate和shipdate在实际应用中的语义，我们认为shipdate >= orderdate这一性质是成立的；
2. 在实际电商应用中，要求商家在订单提交后一段时间内必须发货，否则该订单无效。比如，天猫要求商家在订单提交后72小时内（3天）发货，否则需要承担延迟发货责任。
因此，从实际的语义上来看，shipdate和orderdate之间的差应该不大于某个值（该值通常较小），即shipdate <= orderdate + max_diff。
根据对dbgen文档的解读以及对所提供输入数据的profiling结果，我们发现该max_diff的值约为122.
结合orderdate和shipdate在实际应用中的语义限制，我们认为shipdate <= orderdate + max_diff这一性质是成立的。

综上，我们可以得到，orderdate和shipdate满足特定的关系：
orderdate <= shipdate <= orderdate + max_diff.

#### 剪枝策略分析
由于查询本身的筛选条件中包含了“orderdate < q_orderdate”、"shipdate > q_shipdate"的要求，我们可以对查询的q_orderdate和q_shipdate的不同大小情况进行分类讨论，找到剪枝机会：
1. q_orderdate + max_diff <= q_shipdate : 此时可以根据orderdate对lineitem进行分类讨论：
    - orderdate < q_orderdate ： 此时有shipdate <= orderdate + max_diff < q_orderdate + max_diff <= q_shipdate - max_diff + max_diff = q_shipdate，即shipdate <= q_shipdate。
    因此，这种情况下，没有任何lineitem能够同时满足所有查询条件，查询结果为空，可以进行剪枝，直接返回空结果；
    - q_orderdate <= orderdate : 此时不满足查询条件，直接返回空结果。

    综上，对该查询进行剪枝，直接返回空结果；

2. q_orderdate <= q_shipdate < q_orderdate + max_diff : 此时根据orderdate对lineitem进行分类讨论如下：
    - orderdate <= q_shipdate - max_diff : 此时有shipdate <= orderdate + max_diff <= q_shipdate - max_diff + max_diff = q_shipdate, 即shipdate <= q_shipdate，不满足查询条件，可以进行剪枝，对此区间内的lineitem直接返回空结果；
    - q_shipdate - max_diff < orderdate <= q_orderdate : 此时无法进行剪枝，对此区间内的lineitem进行全体查询；
    - q_orderdate <= orderdate : 此时不满足查询条件，直接返回空结果。

    综上，对该查询进行剪枝，只对满足q_shipdate - max_diff < orderdate <= q_orderdate的lineitem区间进行查询。

3. q_shipdate < q_orderdate : 此时根据orderdate对lineitem进行分类讨论如下：
    - orderdate <= q_shipdate - max_diff : 此时有shipdate <= orderdate + max_diff <= q_shipdate - max_diff + max_diff = q_shipdate, 即shipdate <= q_shipdate，不满足查询条件，可以进行剪枝，对此区间内的lineitem直接返回空结果；
    - q_shipdate - max_diff < orderdate <= q_shipdate : 此时无法进行剪枝，对此区间内的lineitem进行全体查询；
    - q_shipdate < orderdate < q_orderdate : 此时有q_shipdate < orderdate <= shipdate, 即q_shipdate < shipdate，该区间内所有的lineitem都一定满足query的要求，可以静态地直接取该区间内所有lineitem对应的orkerkey和expenditure总和，无需进行单独的shipdate判断查询；
    - q_orderdate <= orderdate : 此时不满足查询条件，直接返回空结果。

    综上，对该查询进行剪枝，只对满足q_shipdate - max_diff < orderdate <= q_orderdate的lineitem区间进行查询，同时可以静态地接受满足q_shipdate < orderdate < q_orderdate的lineitem区间内的所有order。

从上述的剪枝策略分析中，我们发掘了两种剪枝机会：一种是，某个orderdate区间的lineitem不满足查询条件，可以直接返回空结果，即跳过查询；另一种是，某个orderdate区间的lineitem全部满足查询条件，可以在预处理后，查询时将该区间内的topn条结果直接取出，从而跳过查询。
我们将第二种剪枝机会称为pretopn剪枝，并在编码中对这种剪枝进行支持。

#### 剪枝策略通用性
我们的剪枝策略只需要orderdate和shipdate之间具有特定的关系，而对这种关系本身没有明确要求。
即使二者之间的关系并不是orderdate <= shipdate <= orderdate + max_diff，而是另外某种形式的大小关系，我们仍然可以通过剪枝策略分析来获得剪枝空间。
因此，以上的基于区间分析的剪枝策略具有通用性，能够充分发掘数据特点。

### 三 编码方法
根据视图设计和剪枝策略，我们设计了视图的分桶策略、lineitem的编码方法，以及对pretopn剪枝的预处理支持。

#### 分桶策略
根据视图设计，首先我们以marketsegment和orderdate对所有的lineitem进行分桶。
同一个marketsegment下的每连续4个（4为可配置参数）orderdate的lineitem被分到一个桶中，作为一个称为bucket的查询单元。

此外，根据dbgen文档和对数据进行profiling的结果，我们发现，同一个orderkey下可能有1\~7个lineitem。
根据这一数据特性，在视图设计所要求的分桶以外，为了加速查询，我们还根据同一个order的lineitem数量的不同，对lineitem进行进一步的分桶。
由于最终结果要求保留topn个条目，在查询时需要维护一个大小为topn的堆，如果向堆中先插入较大的条目，再插入较小的条目，可以有效减少调整堆的次数，提高查询性能。
而拥有较多lineitem的order，其具有较大的expenditure总和的概率，要高于拥有较少lineitem的order。
因此，我们根据在同order下lineitem数目的不同，将所有的lineitem再划分为三个桶：同order下lineitem数为6\~7的桶（称为major bucket）；同order下lineitem数为4\~5的桶（称为mid bucket）；同order下lineitem数为1\~3的桶（称为minor bucket）。

综上，我们总共在三个维度下对lineitem进行分桶：marketsegment、orderdate、同order下lineitem数量。

#### lineitem编码
根据视图设计，一条lineitem记录需要包括marketsegment、orderdate、orderkey、shipdate、expenditure.
其中，marketsegment信息已经被分桶信息所包含，不需要显式编码在lineitem中。

我们将同一个order（orderkey相同）下的lineitem称为一组lineitem.
我们的lineitem编码方案为，对同一个order的一组lineitem进行编码。
这样的一组lineitem具有相同的orderkey信息和orderdate信息，但每一条lineitem具有各自的shipdate和expenditure信息。
因此，我们将这一组lineitem通过两种32-bit的token来编码：
- orkerkey token: 记录这组lineitem共用的orderkey以及orderdate信息。
由于同一个bucket桶中包含连续4个orderdate的lineitem，因此我们只需要2-bit来存储orderdate相对于bucket base orderdate的差值，即可通过计算得到orderdate；
orderkey则使用余下的30-bit来存储。
- lineitem token: 记录每一条lineitem的shipdate和expenditure。
由于orderdate <= shipdate <= orderdate + max_diff，且根据对dbgen文档的解读以及对所提供输入数据的profiling结果，我们发现该max_diff的值约为122，因此我们可以使用8-bit来记录shipdate相对于orderdate的差值，即可通过计算得到shipdate；
expenditure则使用余下的24-bit来存储。

上述的编码方案中，并没有包含如何区分orderkey token和lineitem token的信息。
在分桶策略中提到，我们根据同order下lineitem的数量，分别将lineitem数位6~7、4~5、1~3分到了major bucket、mid bucket和minor bucket桶中。
为了将各个桶中同order下的lineitem进行对齐，方便进行AVX指令加速，我们将同一个桶中的一组lineitem用0进行padding到相同数量。
比如，在major bucket中，lineitem数为6的会被padding到数目为7；在minor bucket中，lineitem数为1的会被padding为3.
这样，同一个bucket中的一组lineitem中会有固定数目的lineitem token，使得我们可以直接规定出一组lineitem中lineitem token和orderkey token的位置。
比如，我们规定，在一个major bucket中，每一组lineitem的大小为8\*32-bit，其中前面7\*32-bit为7条lineitem token（lineitem数不足的用0补齐），最后32-bit为orderkey token。
这种编码防止了对orderkey item进行判断的开销，且非常便于AVX计算，同时可以在token编码中节省比特位，使得我们可以对所提供的输入数据规模将一个lineitem压缩进32-bit中。
虽然在各个bucket中用0进行padding会带来存储开销，但是我们认为相对于padding带来的编码优势和计算优势，这种开销是值得的。

#### pretopn编码
根据剪枝策略，我们发掘出进行pretopn剪枝的机会，即对某个orderdate区间内的所有lineitem进行处理，无需对shipdate进行判断，直接将同orderkey下的每组lineitem的expenditure进行求和，然后取其中的topn条，作为该区间的查询结果。
为了支持pretopn剪枝，我们需要进行pretopn预处理。

我们将同一个marketsegment下连续8个（8为可配置参数）bucket的lineitem分进一个桶中，称为一个plate。
一个plate中的orderdate跨度为4*8=32.
一条pretopn对应于一组lineitem的信息，需要记录该组lineitem的orderdate、orderkey、totalexpenditure。
对于一组lineitem，我们使用一个64-bit的pretopn token来存储该组lineitem的pretopn信息。
其中，最高的28位用于记录该条pretopn的totalexpenditure；接下来的30位用于记录该条pretopn的orderkey；最低的6位用于记录orderdate与plate base orderdate的偏移量，用于计算出orderdate。

对于每一个plate桶，我们按顺序保存其102400（可配置参数）条totalexpenditure最大的pretopn记录。
在进行查询时，如果该查询符合pretopn剪枝的条件，且其q_topn <= 102400，则我们对该区间直接进行pretopn的扫描，而不去对lineitem bucket进行查询。

由于在pretopn的编码中，需要对同一个plate内的expenditure进行排序，且需要将plate中的pretopn tokens进行存储，因此pretopn剪枝在预处理时会带来额外的计算开销和存储开销。
但和pretopn剪枝能够带来的查询性能优化相比，这些开销是可以接受的。

#### minor bucket键值编码
minor bucket中的lineitem组不太可能会被保留在topn的结果中，因此我们可以为每一个minor bucket记录一个32-bit的键值，这个键值保存的就是这一个minor bucket中最大的totalexpenditure。
在查询时，该键值可以方便我们在扫描minor bucket前进行剪枝。
如果当前的结果堆中的最小的totalexpenditure大于当前minor bucket的键值中的totalexpenditure，则该minor bucket可以直接跳过扫描。


#### 编码方法通用性
上述的许多编码方法充分利用了现有数据的特性（如数据范围等），但即使数据进一步扩大，我们仍然可以通过扩大每一个token的比特数的方法（如将lineitem token从32-bit拓展为64-bit），从而兼容更大的数据规模。
因此，上述的编码方法在具有针对现有输入数据规模的专用性的同时，其设计方法也具有通用性。

### 四 查询过程
1. 对查询进行区间分析，制定剪枝后的查询计划。
此时制定的查询计划可能会将查询划分成多个orderdate区间，如pretopn区间，非pretopn区间等。
这些区间将在后续的步骤中分别进行查询。
2. 扫描pretopn区间对应的plate桶，进行pretopn剪枝。
如果某个查询具有pretopn区间，则首先对其进行pretopn剪枝，使得其结果堆中能够先积累起较大的查询结果，减少后续的调整堆的次数；
3. 进行major bucket的扫描和查询。
首先扫描major bucket，仍然是为了让结果堆中先积累起较大的查询结果。
4. 进行mid bucket的扫描和查询。
5. 进行minor bucket的扫描和查询。
在扫描minor bucket前，首先读取该minor bucket的键值，如果当前的结果堆中的最小的totalexpenditure大于当前minor bucket的键值中的totalexpenditure，则该minor bucket可以直接跳过扫描；否则，对该minor bucket进行扫描和查询。

### 五 工程优化
to be added...



