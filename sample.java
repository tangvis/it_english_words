package cn.sunline.ams.batch.s1000;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import com.querydsl.jpa.impl.JPAQuery;

import cn.sunline.ams.context.RepaySchedule;
import cn.sunline.ams.definition.enums.OrderOwner;
import cn.sunline.ams.definition.enums.OrderStatus;
import cn.sunline.ams.definition.enums.OrderType;
import cn.sunline.ams.definition.enums.TxnDirection;
import cn.sunline.ams.definition.enums.TxnProcessType;
import cn.sunline.ams.definition.enums.TxnType;
import cn.sunline.ams.facility.LoanFacility;
import cn.sunline.ams.facility.OrderTxnFacility;
import cn.sunline.ams.facility.ScheduleFacility;
import cn.sunline.ams.infrastructure.server.repos.RAmsTxnOrder;
import cn.sunline.ams.infrastructure.shared.model.AmsLoan;
import cn.sunline.ams.infrastructure.shared.model.AmsPlan;
import cn.sunline.ams.infrastructure.shared.model.AmsTxnOrder;
import cn.sunline.ams.infrastructure.shared.model.QAmsPlan;
import cn.sunline.batch.task.reader.file.struct.LineItem;
import cn.sunline.batch.utils.BatchStatusFacility;
import cn.sunline.common.KC;

/**
 * 分期贷款逾期批扣
 *
 * @author lsf
 * @version [产品/模块]
 * @see [相关类/方法]
 * @since [JDK 1.8]
 *        <p>
 *        [2019年02月14日]
 */
public class P1010OverdueRepay implements ItemProcessor<LineItem<String>, String> {

	private static final Logger logger = LoggerFactory.getLogger(P1010OverdueRepay.class);

	@PersistenceContext
	private EntityManager em;

	@Autowired
	private LoanFacility loanFacility;

	@Autowired
	private ScheduleFacility scheduleFacility;

	@Autowired
	private BatchStatusFacility batchFacility;

	@Autowired
	private OrderTxnFacility orderTxnFacility;

	QAmsPlan qAmsPlan = QAmsPlan.amsPlan;
	@Autowired
	RAmsTxnOrder rAmsTxnOrder;

	@Override
	public String process(LineItem<String> itemLoanId) throws Exception {
		String loanId = itemLoanId.getLineObject();
		logger.info("生成逾期订单, loanId:[{}]", loanId);
		// 获取借据及当期还款计划
		AmsLoan loan = em.find(AmsLoan.class, loanId);
		RepaySchedule schedule = scheduleFacility.queryRepaySchedule(loanId, loan.getCurrTerm());
		// 未还
		BigDecimal overdueAmt = loanFacility.calLoanOverdueAmt(loan, null,
				batchFacility.getSystemStatus().getBizDate());
		// 当期应还
		BigDecimal termAmt = BigDecimal.ZERO;
		BigDecimal termPremiumFee = BigDecimal.ZERO;
		BigDecimal termCapitalAmt = BigDecimal.ZERO;
		if (DateUtils.isSameDay(batchFacility.getSystemStatus().getBizDate(),
				schedule.getAmsRepaySchedule().getDueDate())) {
			termAmt = schedule.getAmsRepaySchedule().getTermAmt();
			termPremiumFee = schedule.getAmsRepaySchedule().getTermPremiumFee();
			termCapitalAmt = termAmt.subtract(termPremiumFee);
		}
		// 溢缴款
		BigDecimal overflowAmt = loan.getOverflowAmt();

		// 应还 = 当期应还 + 当期应还(还款日当天) - 溢缴款
		BigDecimal repayAmt = overdueAmt.add(termAmt).subtract(overflowAmt);

		// 已还完
		if (repayAmt.compareTo(BigDecimal.ZERO) <= 0) {
			return loanId;
		}
		// 处理中订单
		if (orderTxnFacility.hasProcessedOrder(loanId)) {
			return loanId;
		}
		// 按照plan一期一的出订单
		String preOrderId = createOverDueOrderByPlan(loan, repayAmt);
		// 因为正常的订单是最后出的，期数肯定是最后一期
		if (DateUtils.isSameDay(batchFacility.getSystemStatus().getBizDate(),
				schedule.getAmsRepaySchedule().getDueDate())) {
			createNormalOrder(loan, termAmt, termPremiumFee, termCapitalAmt, preOrderId,
					schedule.getAmsRepaySchedule().getTerm());
		}

		return null;
	}

	/**
	 * 逻辑是一期一期出订单 并且都是先扣保费后扣银行本息 前端查询时都是看到的主订单号，所以一期生成了三个订单
	 * 出一个主订单，两个拆分订单（一个总的，一个保费，一个银行本息）
	 * 
	 * @param loan
	 * @param repayAmt
	 * @return
	 */
	private String createOverDueOrderByPlan(AmsLoan loan, BigDecimal repayAmt) {
		// 前置订单号
		// 产生最后一个主订单，给后续的账单日当天的前置订单
		String preOrderId = "";
		// 升序排序
		List<AmsPlan> planList = new JPAQuery<AmsPlan>(em).from(qAmsPlan)
				.where(qAmsPlan.loanId.eq(loan.getLoanId()).and(qAmsPlan.term.loe(loan.getCurrTerm())))
				.orderBy(qAmsPlan.term.asc()).fetch();
		// 宽限期
		Boolean waiveInt = loanFacility.isInGraceDate(loan, batchFacility.getSystemStatus().getBizDate());
		// 按照plan一期一期的出订单
		for (AmsPlan plan : planList) {
			BigDecimal currBal = plan.getCurrBal().add(plan.getIntAccur());
			if (!waiveInt) {
				currBal = currBal.add(plan.getOverIntAccur());
			}
			BigDecimal captailAmt = currBal.subtract(plan.getPremiumFee()).setScale(2, RoundingMode.HALF_UP);
			if (currBal.compareTo(BigDecimal.ZERO) > 0) {
				// 主订单标识
				AmsTxnOrder mainTxn = orderTxnFacility.createOrder(loan, loan.getAcctId(), repayAmt, TxnType.OC,
						TxnDirection.C, TxnProcessType.Batch, null, batchFacility.getSystemStatus().getBizDate(),
						OrderOwner.MEMO, OrderType.ORI, plan.getTerm());
				// 主订单直接W中，再支付回盘的时候，修改
				mainTxn.setOrderStatus(OrderStatus.W.name());
				mainTxn = rAmsTxnOrder.save(mainTxn);
				// 保费
				if (plan.getPremiumFee().compareTo(BigDecimal.ZERO) > 0) {
					AmsTxnOrder premiumFeeOrder = orderTxnFacility.createOrder(loan, loan.getAcctId(),
							plan.getPremiumFee(), TxnType.OC, TxnDirection.C, TxnProcessType.Batch, null,
							batchFacility.getSystemStatus().getBizDate(), OrderOwner.SELF, OrderType.SPLIT,
							plan.getTerm());
					premiumFeeOrder.setMainOrderNo(mainTxn.getOrderId());
					if (KC.string.isNotBlank(preOrderId)) {
						premiumFeeOrder.setPreOrderNo(preOrderId);
					}
					premiumFeeOrder = rAmsTxnOrder.save(premiumFeeOrder);
					preOrderId = premiumFeeOrder.getOrderId();
				}
				// 资金方金额
				if (captailAmt.compareTo(BigDecimal.ZERO) > 0) {
					AmsTxnOrder capitalOrder = orderTxnFacility.createOrder(loan, loan.getAcctId(), captailAmt,
							TxnType.OC, TxnDirection.C, TxnProcessType.Batch, null,
							batchFacility.getSystemStatus().getBizDate(), OrderOwner.CAPITAL, OrderType.SPLIT,
							plan.getTerm());
					capitalOrder.setMainOrderNo(mainTxn.getOrderId());
					if (KC.string.isNotBlank(preOrderId)) {
						capitalOrder.setPreOrderNo(preOrderId);
					}
					capitalOrder = rAmsTxnOrder.save(capitalOrder);
					preOrderId = capitalOrder.getOrderId();
				}

			}
		}
		return preOrderId;
	}

	/**
	 * 逾期当天出正常的订单
	 * 
	 * @param loan
	 * @param termAmt
	 * @param termPremiumFee
	 * @param termCapitalAmt
	 * @param preOrderId
	 */
	private void createNormalOrder(AmsLoan loan, BigDecimal termAmt, BigDecimal termPremiumFee,
			BigDecimal termCapitalAmt, String preOrderId, int term) {
		// 主订单标识
		AmsTxnOrder mainTxn = orderTxnFacility.createOrder(loan, loan.getAcctId(), termAmt, TxnType.NC, TxnDirection.C,
				TxnProcessType.Batch, null, batchFacility.getSystemStatus().getBizDate(), OrderOwner.MEMO,
				OrderType.ORI, term);
		// 因为批量的主订单没必要有状态
		mainTxn.setOrderStatus(OrderStatus.W.name());
		mainTxn = rAmsTxnOrder.save(mainTxn);
		// 保险订单
		AmsTxnOrder premiumFeeOrder = null;
		if (termPremiumFee.compareTo(BigDecimal.ZERO) > 0) {
			premiumFeeOrder = orderTxnFacility.createOrder(loan, loan.getAcctId(), termPremiumFee, TxnType.NC,
					TxnDirection.C, TxnProcessType.Batch, null, batchFacility.getSystemStatus().getBizDate(),
					OrderOwner.SELF, OrderType.SPLIT, term);
			premiumFeeOrder.setMainOrderNo(mainTxn.getOrderId());
			if (KC.string.isNotBlank(preOrderId)) {
				premiumFeeOrder.setPreOrderNo(preOrderId);
			}
			premiumFeeOrder = rAmsTxnOrder.save(premiumFeeOrder);
		}

		// 资金方订单
		if (termCapitalAmt.compareTo(BigDecimal.ZERO) > 0) {
			AmsTxnOrder capitalOrder = orderTxnFacility.createOrder(loan, loan.getAcctId(), termCapitalAmt, TxnType.NC,
					TxnDirection.C, TxnProcessType.Batch, null, batchFacility.getSystemStatus().getBizDate(),
					OrderOwner.CAPITAL, OrderType.SPLIT, term);
			capitalOrder.setMainOrderNo(mainTxn.getOrderId());
			if(premiumFeeOrder != null){
				capitalOrder.setPreOrderNo(premiumFeeOrder.getOrderId());
			}
			rAmsTxnOrder.save(capitalOrder);
		}
	}

}
