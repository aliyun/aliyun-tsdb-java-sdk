package com.aliyun.hitsdb.client;

import java.io.IOException;

import com.aliyun.hitsdb.client.callback.LoadCallback;
import com.aliyun.hitsdb.client.exception.http.HttpClientInitException;
import com.aliyun.hitsdb.client.value.Result;
import com.aliyun.hitsdb.client.value.request.CompressionBatchPoints;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHiTSDBClientLoad {
    HiTSDB tsdb;

    @Before
    public void init() throws HttpClientInitException {
        LoadCallback lcb = new LoadCallback() {

            @Override
            public void response(String address, CompressionBatchPoints points, Result result) {
                System.out.println("已处理" + points.size() + "个点");
            }
            
            @Override
            public void failed(String address, CompressionBatchPoints points, Exception ex) {
                System.err.println("业务回调出错！" + points.size() + " error!");
                ex.printStackTrace();
            }
        };

        HiTSDBConfig config = HiTSDBConfig.address("127.0.0.1", 3242)
            .listenLoad(lcb)
            .httpConnectTimeout(90)
            .config();
        tsdb = HiTSDBClientFactory.connect(config);
    }

    @After
    public void after() {
        try {
            System.out.println("将要关闭");
            tsdb.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutData() {
        CompressionBatchPoints.MetricBuilder builder = CompressionBatchPoints.metric("load-metric").tag("asdf","asdf");
        builder.appendDouble(1500405481623L, 69087);
        builder.appendDouble(1500405488693L, 65640);
        builder.appendDouble(1500405495993L, 58155);
        builder.appendDouble(1500405503743L, 61025);
        builder.appendDouble(1500405511623L, 91156);
        builder.appendDouble(1500405519803L, 37516);
        builder.appendDouble(1500405528313L, 93515);
        builder.appendDouble(1500405537233L, 96226);
        builder.appendDouble(1500405546453L, 23833);
        builder.appendDouble(1500405556103L, 73186);
        builder.appendDouble(1500405566143L, 96947);
        builder.appendDouble(1500405576163L, 46927);
        builder.appendDouble(1500405586173L, 77954);
        builder.appendDouble(1500405596183L, 29302);
        builder.appendDouble(1500405606213L, 6700);
        builder.appendDouble(1500405616163L, 71971);
        builder.appendDouble(1500405625813L, 8528);
        builder.appendDouble(1500405635763L, 85321);
        builder.appendDouble(1500405645634L, 83229);
        builder.appendDouble(1500405655633L, 78298);
        builder.appendDouble(1500405665623L, 87122);
        builder.appendDouble(1500405675623L, 82055);
        builder.appendDouble(1500405685723L, 75067);
        builder.appendDouble(1500405695663L, 33680);
        builder.appendDouble(1500405705743L, 17576);
        builder.appendDouble(1500405715813L, 89701);
        builder.appendDouble(1500405725773L, 21427);
        builder.appendDouble(1500405735883L, 58255);
        builder.appendDouble(1500405745903L, 3768);
        builder.appendDouble(1500405755863L, 62086);
        builder.appendDouble(1500405765843L, 66965);
        builder.appendDouble(1500405775773L, 35801);
        builder.appendDouble(1500405785883L, 72169);
        builder.appendDouble(1500405795843L, 43089);
        builder.appendDouble(1500405805733L, 31418);
        builder.appendDouble(1500405815853L, 84781);
        builder.appendDouble(1500405825963L, 36103);
        builder.appendDouble(1500405836004L, 87431);
        builder.appendDouble(1500405845953L, 7379);
        builder.appendDouble(1500405855913L, 66919);
        builder.appendDouble(1500405865963L, 30906);
        builder.appendDouble(1500405875953L, 88630);
        builder.appendDouble(1500405885943L, 27546);
        builder.appendDouble(1500405896033L, 43813);
        builder.appendDouble(1500405906094L, 2124);
        builder.appendDouble(1500405916063L, 49399);
        builder.appendDouble(1500405926143L, 94577);
        builder.appendDouble(1500405936123L, 98459);
        builder.appendDouble(1500405946033L, 49457);
        builder.appendDouble(1500405956023L, 92838);
        builder.appendDouble(1500405966023L, 15628);
        builder.appendDouble(1500405976043L, 53916);
        builder.appendDouble(1500405986063L, 90387);
        builder.appendDouble(1500405996123L, 43176);
        builder.appendDouble(1500406006123L, 18838);
        builder.appendDouble(1500406016174L, 78847);
        builder.appendDouble(1500406026173L, 39591);
        builder.appendDouble(1500406036004L, 77070);
        builder.appendDouble(1500406045964L, 56788);
        builder.appendDouble(1500406056043L, 96706);
        builder.appendDouble(1500406066123L, 20756);
        builder.appendDouble(1500406076113L, 64433);
        builder.appendDouble(1500406086133L, 45791);
        builder.appendDouble(1500406096123L, 75028);
        builder.appendDouble(1500406106193L, 55403);
        builder.appendDouble(1500406116213L, 36991);
        builder.appendDouble(1500406126073L, 92929);
        builder.appendDouble(1500406136103L, 60416);
        builder.appendDouble(1500406146183L, 55485);
        builder.appendDouble(1500406156383L, 53525);
        builder.appendDouble(1500406166313L, 96021);
        builder.appendDouble(1500406176414L, 22705);
        builder.appendDouble(1500406186613L, 89801);
        builder.appendDouble(1500406196543L, 51975);
        builder.appendDouble(1500406206483L, 86741);
        builder.appendDouble(1500406216483L, 22440);
        builder.appendDouble(1500406226433L, 51818);
        builder.appendDouble(1500406236403L, 61965);
        builder.appendDouble(1500406246413L, 19074);
        builder.appendDouble(1500406256494L, 54521);
        builder.appendDouble(1500406266413L, 59315);
        builder.appendDouble(1500406276303L, 19171);
        builder.appendDouble(1500406286213L, 98800);
        builder.appendDouble(1500406296183L, 7086);
        builder.appendDouble(1500406306103L, 60578);
        builder.appendDouble(1500406316073L, 96828);
        builder.appendDouble(1500406326143L, 83746);
        builder.appendDouble(1500406336153L, 85481);
        builder.appendDouble(1500406346113L, 22346);
        builder.appendDouble(1500406356133L, 80976);
        builder.appendDouble(1500406366065L, 43586);
        builder.appendDouble(1500406376074L, 82500);
        builder.appendDouble(1500406386184L, 13576);
        builder.appendDouble(1500406396113L, 77871);
        builder.appendDouble(1500406406094L, 60978);
        builder.appendDouble(1500406416203L, 35264);
        builder.appendDouble(1500406426323L, 79733);
        builder.appendDouble(1500406436343L, 29140);
        builder.appendDouble(1500406446323L, 7237);
        builder.appendDouble(1500406456344L, 52866);
        builder.appendDouble(1500406466393L, 88456);
        builder.appendDouble(1500406476493L, 33533);
        builder.appendDouble(1500406486524L, 96961);
        builder.appendDouble(1500406496453L, 16389);
        builder.appendDouble(1500406506453L, 31181);
        builder.appendDouble(1500406516433L, 63282);
        builder.appendDouble(1500406526433L, 92857);
        builder.appendDouble(1500406536413L, 4582);
        builder.appendDouble(1500406546383L, 46832);
        builder.appendDouble(1500406556473L, 6335);
        builder.appendDouble(1500406566413L, 44367);
        builder.appendDouble(1500406576513L, 84640);
        builder.appendDouble(1500406586523L, 36174);
        builder.appendDouble(1500406596553L, 40075);
        builder.appendDouble(1500406606603L, 80886);
        builder.appendDouble(1500406616623L, 43784);
        builder.appendDouble(1500406626623L, 25077);
        builder.appendDouble(1500406636723L, 18617);
        builder.appendDouble(1500406646723L, 72681);
        builder.appendDouble(1500406656723L, 84811);
        builder.appendDouble(1500406666783L, 90053);
        builder.appendDouble(1500406676685L, 25708);
        builder.appendDouble(1500406686713L, 57134);
        builder.appendDouble(1500406696673L, 87193);
        builder.appendDouble(1500406706743L, 66057);
        builder.appendDouble(1500406716724L, 51404);
        builder.appendDouble(1500406726753L, 90141);
        builder.appendDouble(1500406736813L, 10434);
        builder.appendDouble(1500406746803L, 29056);
        builder.appendDouble(1500406756833L, 48160);
        builder.appendDouble(1500406766924L, 96652);
        builder.appendDouble(1500406777113L, 64141);
        builder.appendDouble(1500406787113L, 22143);
        builder.appendDouble(1500406797093L, 20561);
        builder.appendDouble(1500406807113L, 66401);
        builder.appendDouble(1500406817283L, 76802);
        builder.appendDouble(1500406827284L, 37555);
        builder.appendDouble(1500406837323L, 63169);
        builder.appendDouble(1500406847463L, 45712);
        builder.appendDouble(1500406857513L, 44751);
        builder.appendDouble(1500406867523L, 98891);
        builder.appendDouble(1500406877523L, 38122);
        builder.appendDouble(1500406887623L, 46202);
        builder.appendDouble(1500406897703L, 5875);
        builder.appendDouble(1500406907663L, 17397);
        builder.appendDouble(1500406917603L, 39994);
        builder.appendDouble(1500406927633L, 82385);
        builder.appendDouble(1500406937623L, 15598);
        builder.appendDouble(1500406947693L, 36235);
        builder.appendDouble(1500406957703L, 97536);
        builder.appendDouble(1500406967673L, 28557);
        builder.appendDouble(1500406977723L, 13985);
        builder.appendDouble(1500406987663L, 64304);
        builder.appendDouble(1500406997573L, 83693);
        builder.appendDouble(1500407007494L, 6574);
        builder.appendDouble(1500407017493L, 25134);
        builder.appendDouble(1500407027503L, 50383);
        builder.appendDouble(1500407037523L, 55922);
        builder.appendDouble(1500407047603L, 73436);
        builder.appendDouble(1500407057473L, 68235);
        builder.appendDouble(1500407067553L, 1469);
        builder.appendDouble(1500407077463L, 44315);
        builder.appendDouble(1500407087463L, 95064);
        builder.appendDouble(1500407097443L, 1997);
        builder.appendDouble(1500407107473L, 17247);
        builder.appendDouble(1500407117453L, 42454);
        builder.appendDouble(1500407127413L, 73631);
        builder.appendDouble(1500407137363L, 96890);
        builder.appendDouble(1500407147343L, 43450);
        builder.appendDouble(1500407157363L, 42042);
        builder.appendDouble(1500407167403L, 83014);
        builder.appendDouble(1500407177473L, 32051);
        builder.appendDouble(1500407187523L, 69280);
        builder.appendDouble(1500407197495L, 21425);
        builder.appendDouble(1500407207453L, 93748);
        builder.appendDouble(1500407217413L, 64151);
        builder.appendDouble(1500407227443L, 38791);
        builder.appendDouble(1500407237463L, 5248);
        builder.appendDouble(1500407247523L, 92935);
        builder.appendDouble(1500407257513L, 18516);
        builder.appendDouble(1500407267584L, 98870);
        builder.appendDouble(1500407277573L, 82244);
        builder.appendDouble(1500407287723L, 65464);
        builder.appendDouble(1500407297723L, 33801);
        builder.appendDouble(1500407307673L, 18331);
        builder.appendDouble(1500407317613L, 89744);
        builder.appendDouble(1500407327553L, 98460);
        builder.appendDouble(1500407337503L, 24709);
        builder.appendDouble(1500407347423L, 8407);
        builder.appendDouble(1500407357383L, 69451);
        builder.appendDouble(1500407367333L, 51100);
        builder.appendDouble(1500407377373L, 25309);
        builder.appendDouble(1500407387443L, 16148);
        builder.appendDouble(1500407397453L, 98974);
        builder.appendDouble(1500407407543L, 80284);
        builder.appendDouble(1500407417583L, 170);
        builder.appendDouble(1500407427453L, 34706);
        builder.appendDouble(1500407437433L, 39681);
        builder.appendDouble(1500407447603L, 6140);
        builder.appendDouble(1500407457513L, 64595);
        builder.appendDouble(1500407467564L, 59862);
        builder.appendDouble(1500407477563L, 53795);
        builder.appendDouble(1500407487593L, 83493);
        builder.appendDouble(1500407497584L, 90639);
        builder.appendDouble(1500407507623L, 16777);
        builder.appendDouble(1500407517613L, 11096);
        builder.appendDouble(1500407527673L, 38512);
        builder.appendDouble(1500407537963L, 52759);
        builder.appendDouble(1500407548023L, 79567);
        builder.appendDouble(1500407558033L, 48664);
        builder.appendDouble(1500407568113L, 10710);
        builder.appendDouble(1500407578164L, 25635);
        builder.appendDouble(1500407588213L, 40985);
        builder.appendDouble(1500407598163L, 94089);
        builder.appendDouble(1500407608163L, 50056);
        builder.appendDouble(1500407618223L, 15550);
        builder.appendDouble(1500407628143L, 78823);
        builder.appendDouble(1500407638223L, 9044);
        builder.appendDouble(1500407648173L, 20782);
        builder.appendDouble(1500407658023L, 86390);
        builder.appendDouble(1500407667903L, 79444);
        builder.appendDouble(1500407677903L, 84051);
        builder.appendDouble(1500407687923L, 91554);
        builder.appendDouble(1500407697913L, 58777);
        builder.appendDouble(1500407708003L, 89474);
        builder.appendDouble(1500407718083L, 94026);
        builder.appendDouble(1500407728034L, 41613);
        builder.appendDouble(1500407738083L, 64667);
        builder.appendDouble(1500407748034L, 5160);
        builder.appendDouble(1500407758003L, 45140);
        builder.appendDouble(1500407768033L, 53704);
        builder.appendDouble(1500407778083L, 68097);
        builder.appendDouble(1500407788043L, 81137);
        builder.appendDouble(1500407798023L, 59657);
        builder.appendDouble(1500407808033L, 56572);
        builder.appendDouble(1500407817983L, 1993);
        builder.appendDouble(1500407828063L, 62608);
        builder.appendDouble(1500407838213L, 76489);
        builder.appendDouble(1500407848203L, 22147);
        builder.appendDouble(1500407858253L, 92829);
        builder.appendDouble(1500407868073L, 48499);
        builder.appendDouble(1500407878053L, 89152);
        builder.appendDouble(1500407888073L, 9191);
        builder.appendDouble(1500407898033L, 49881);
        builder.appendDouble(1500407908113L, 96020);
        builder.appendDouble(1500407918213L, 90203);
        builder.appendDouble(1500407928234L, 32217);
        builder.appendDouble(1500407938253L, 94302);
        builder.appendDouble(1500407948293L, 83111);
        builder.appendDouble(1500407958234L, 75576);
        builder.appendDouble(1500407968073L, 5973);
        builder.appendDouble(1500407978023L, 5175);
        builder.appendDouble(1500407987923L, 63350);
        builder.appendDouble(1500407997833L, 44081);
        
        
        CompressionBatchPoints points = builder.build();
        tsdb.load(points);
    }
}
