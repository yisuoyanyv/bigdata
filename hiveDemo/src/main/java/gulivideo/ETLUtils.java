package gulivideo;

/**
 * ETL 工具类
 *
 * gulivideo场景 video数据的清洗规则：
 * 1.数据的长度必须大于等于9
 * 2.将每个视频的类别中的空格去掉
 * 3.将每个关联视频id通过&拼接
 */
public class ETLUtils {

    /**
     * 原始数据：
     * Qehxjub5lyo	jgrandin1	715	Entertainment	177	944984	4.5	3641	1943	LzE2G4qaz9k	EBrm-wz5r6M	PB4a2AAwHnE	pYCs_Rv2dfs	62Opv2W8XJc	G6f9zxSDgD8	oDd0u1bj4ro	L5vHXDXXHyU	F-x_oA96NlI	E7_HITKYlAU	EGHoPrUR3Xg	zcT93NZQ8wE	AzfTM_hQY6c	R_BwlXkWS_M	EmioYVrE3y0	4zxnzztCYbA	FWDw6rpZ4CQ	wph_pmP9XC4	nENB-RfAUn8	DyccrmBwmXc
     * @param data
     * @return
     */
    public static String etlData(String data){
        //1.判断数据的长度
        String[] dataArray = data.split("\t");
        if(dataArray.length < 9){
            //数据不合法
            return null;
        }
        //2.去掉视频类别中的空格
        dataArray[3] = dataArray[3].replaceAll(" ","");
        //3.将每个视频的关联视频id通过&拼接
        StringBuffer sbs = new StringBuffer();
        for (int i = 0; i < dataArray.length; i++) {
            //3.1.处理关联视频之前的数据
            if(i < 9){
                if(i == dataArray.length-1){
                    //没有关联视频
                    sbs.append(dataArray[i]);
                }else {
                    sbs.append(dataArray[i]).append("\t");
                }
            }else {
                //处理关联视频
                if(i == dataArray.length-1){
                    sbs.append(dataArray[i]);
                }else {
                    sbs.append(dataArray[i]).append("&");
                }
            }
        }

        return sbs.toString();

    }

    public static void main(String[] args) {
        String data="Qehxjub5lyo\tjgrandin1\t715\tEntertainment & Fine\t177\t944984\t4.5\t3641\n";
        System.out.println(etlData(data));
    }

}
